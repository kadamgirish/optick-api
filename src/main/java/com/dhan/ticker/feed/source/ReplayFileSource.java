package com.dhan.ticker.feed.source;

import com.dhan.ticker.exception.WebSocketException;
import com.dhan.ticker.feed.TickPipeline;
import com.dhan.ticker.feed.TickStateService;
import com.dhan.ticker.model.IndexInstrument;
import com.dhan.ticker.model.replay.FrameIndexEntry;
import com.dhan.ticker.model.replay.SessionManifest;
import com.dhan.ticker.replay.FrameIndexStream;
import com.dhan.ticker.replay.SessionDiscoveryService;
import com.dhan.ticker.service.MasterDataService;
import jakarta.annotation.PreDestroy;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Stream;

/**
 * File-backed {@link FrameSource} that replays a recorded session's
 * {@code frames.bin} into the shared {@link TickPipeline} paced by the
 * recorded {@code timestampNanos / speed}.
 *
 * <p>Single active session at a time. Decode + enrich + broadcast are
 * performed downstream by exactly the same code path used for live frames —
 * Angular consumers cannot tell live from replay.
 */
@Slf4j
@Component
public class ReplayFileSource implements FrameSource {

    public enum State { IDLE, PLAYING, PAUSED, STOPPED }

    private final SessionDiscoveryService discovery;
    private final FrameIndexStream indexStream;
    private final TickStateService stateService;
    private final TickPipeline pipeline;
    private final MasterDataService masterData;

    private final AtomicReference<State> state = new AtomicReference<>(State.IDLE);
    private final AtomicReference<String> currentSessionId = new AtomicReference<>();
    private final AtomicLong framesPushed = new AtomicLong(0);
    private final AtomicLong currentTimestampNanos = new AtomicLong(0);
    private final AtomicBoolean stopRequested = new AtomicBoolean(false);
    private final Object pauseLock = new Object();

    private volatile Thread playbackThread;
    private volatile FileChannel currentChannel;
    private volatile Stream<FrameIndexEntry> currentIndex;
    private volatile ScheduledExecutorService scheduler;

    public ReplayFileSource(SessionDiscoveryService discovery,
                            FrameIndexStream indexStream,
                            TickStateService stateService,
                            TickPipeline pipeline,
                            MasterDataService masterData) {
        this.discovery = discovery;
        this.indexStream = indexStream;
        this.stateService = stateService;
        this.pipeline = pipeline;
        this.masterData = masterData;
    }

    // ── Public lifecycle ──────────────────────────────────────────────────

    public synchronized void start(String sessionId, double speed) {
        if (state.get() == State.PLAYING || state.get() == State.PAUSED) {
            throw new WebSocketException("Replay already active: session=" + currentSessionId.get()
                    + " state=" + state.get() + ". Call /stop first.");
        }
        if (speed <= 0) speed = 1.0;
        final double effectiveSpeed = speed;

        Path sessionDir = discovery.resolveSessionDir(sessionId);
        SessionManifest manifest = discovery.getManifest(sessionId);

        // Seed instruments + flip into replay mode so isMarketOpen returns true
        // and broadcast follows the same path as live.
        stateService.replayClearSubscribedInstruments();
        stateService.replaySeedSubscribedInstruments(toIndexInstruments(manifest));
        seedSubscriptionGroups(manifest);
        stateService.setReplayMode(true);
        pipeline.resume();

        currentSessionId.set(sessionId);
        framesPushed.set(0);
        currentTimestampNanos.set(0);
        stopRequested.set(false);
        state.set(State.PLAYING);

        scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "replay-" + sessionId);
            t.setDaemon(true);
            return t;
        });

        Thread t = new Thread(() -> runPlayback(sessionDir, effectiveSpeed),
                "replay-driver-" + sessionId);
        t.setDaemon(true);
        playbackThread = t;
        t.start();

        log.info("[REPLAY] Started session={} speed={}x instruments={}",
                sessionId, speed, manifest.getInstruments() == null ? 0 : manifest.getInstruments().size());
    }

    public synchronized void pause() {
        if (state.get() != State.PLAYING) return;
        state.set(State.PAUSED);
        log.info("[REPLAY] Paused session={}", currentSessionId.get());
    }

    public synchronized void resume() {
        if (state.get() != State.PAUSED) return;
        state.set(State.PLAYING);
        synchronized (pauseLock) { pauseLock.notifyAll(); }
        log.info("[REPLAY] Resumed session={}", currentSessionId.get());
    }

    @Override
    public synchronized void stop() {
        if (state.get() == State.IDLE || state.get() == State.STOPPED) return;
        stopRequested.set(true);
        synchronized (pauseLock) { pauseLock.notifyAll(); }
        Thread t = playbackThread;
        if (t != null) {
            t.interrupt();
            try { t.join(2000); } catch (InterruptedException ignored) { Thread.currentThread().interrupt(); }
        }
        cleanup();
        state.set(State.STOPPED);
        stateService.setReplayMode(false);
        stateService.replayClearSubscribedInstruments();
        log.info("[REPLAY] Stopped");
    }

    @Override
    public boolean isOpen() {
        State s = state.get();
        return s == State.PLAYING || s == State.PAUSED;
    }

    public State getState()             { return state.get(); }
    public String getCurrentSessionId() { return currentSessionId.get(); }
    public long getFramesPushed()       { return framesPushed.get(); }
    public long getCurrentTimestampNanos() { return currentTimestampNanos.get(); }

    @PreDestroy
    public void shutdown() {
        try { stop(); } catch (Exception ignored) {}
    }

    // ── Internals ────────────────────────────────────────────────────────

    private void runPlayback(Path sessionDir, double speed) {
        long firstTs = -1L;
        long startNanos = System.nanoTime();
        try (FileChannel ch = FileChannel.open(sessionDir.resolve("frames.bin"), StandardOpenOption.READ);
             Stream<FrameIndexEntry> idx = indexStream.stream(sessionDir)) {

            currentChannel = ch;
            currentIndex = idx;

            // Pre-seed baseline state (PrevClose=6, OI=5) so that gates in
            // TickStateService — e.g. INDEX waiting on prevDayClose, futures
            // OI baseline — open immediately instead of after frames arrive in
            // recorded order. Reads the index file once, no pacing.
            preSeedBaselineFrames(sessionDir, ch);

            Iterator<FrameIndexEntry> it = idx.iterator();

            while (it.hasNext()) {
                if (stopRequested.get()) break;

                while (state.get() == State.PAUSED && !stopRequested.get()) {
                    synchronized (pauseLock) {
                        try { pauseLock.wait(250); }
                        catch (InterruptedException ie) { Thread.currentThread().interrupt(); break; }
                    }
                }
                if (stopRequested.get()) break;

                FrameIndexEntry e = it.next();
                if (firstTs < 0) {
                    firstTs = e.timestampNanos();
                    startNanos = System.nanoTime();
                }

                long targetElapsedNanos = (long) ((e.timestampNanos() - firstTs) / speed);
                long actualElapsedNanos = System.nanoTime() - startNanos;
                long sleepNanos = targetElapsedNanos - actualElapsedNanos;
                if (sleepNanos > 0) {
                    try {
                        long ms = sleepNanos / 1_000_000L;
                        int ns = (int) (sleepNanos % 1_000_000L);
                        Thread.sleep(ms, ns);
                    } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                        break;
                    }
                }

                ByteBuffer buf = ByteBuffer.allocate(e.length());
                ch.position(e.offset());
                int total = 0;
                while (total < e.length()) {
                    int n = ch.read(buf);
                    if (n < 0) break;
                    total += n;
                }
                buf.flip();
                pipeline.accept(buf);
                framesPushed.incrementAndGet();
                currentTimestampNanos.set(e.timestampNanos());
            }
        } catch (IOException ioe) {
            log.error("[REPLAY] I/O error: {}", ioe.getMessage(), ioe);
        } catch (Exception ex) {
            log.error("[REPLAY] Unexpected error: {}", ex.getMessage(), ex);
        } finally {
            log.info("[REPLAY] Playback finished session={} framesPushed={}",
                    currentSessionId.get(), framesPushed.get());
            // Auto-transition to STOPPED if we exit naturally without explicit stop()
            if (!stopRequested.get()) {
                state.set(State.STOPPED);
                stateService.setReplayMode(false);
            }
        }
    }

    private void cleanup() {
        ScheduledExecutorService s = scheduler;
        if (s != null) s.shutdownNow();
        scheduler = null;
        playbackThread = null;
        currentChannel = null;
        currentIndex = null;
    }

    /**
     * Walk the index once and push every PrevClose (frameType=6) and
     * OI-baseline (frameType=5) frame through the pipeline, un-paced.
     * Replay processes these the same as live, populating
     * {@code prevDayClose} / {@code prevDayOi} so downstream broadcast
     * gates open before the timed loop begins.
     */
    private void preSeedBaselineFrames(Path sessionDir, FileChannel ch) throws IOException {
        int seeded = 0;
        try (Stream<FrameIndexEntry> idx = indexStream.stream(sessionDir)) {
            Iterator<FrameIndexEntry> it = idx.iterator();
            while (it.hasNext()) {
                if (stopRequested.get()) return;
                FrameIndexEntry e = it.next();
                int t = e.frameType();
                if (t != 5 && t != 6) continue;
                ByteBuffer buf = ByteBuffer.allocate(e.length());
                ch.position(e.offset());
                int total = 0;
                while (total < e.length()) {
                    int n = ch.read(buf);
                    if (n < 0) break;
                    total += n;
                }
                buf.flip();
                pipeline.accept(buf);
                seeded++;
            }
        }
        log.info("[REPLAY] Pre-seeded {} baseline frames (PrevClose/OI) for session={}",
                seeded, currentSessionId.get());
    }

    private List<IndexInstrument> toIndexInstruments(SessionManifest manifest) {
        List<IndexInstrument> out = new ArrayList<>();
        if (manifest.getInstruments() == null) return out;
        for (SessionManifest.InstrumentEntry e : manifest.getInstruments()) {
            if (e == null) continue;
            String secId = String.valueOf(e.getSecurityId());
            String tradingSymbol = e.getTradingSymbol();
            String symbol = deriveSymbol(tradingSymbol, e.getInstrumentType());
            IndexInstrument inst = IndexInstrument.builder()
                    .securityId(secId)
                    .exchangeSegment(e.getExchangeSegment())
                    .instrumentType(e.getInstrumentType())
                    .tradingSymbol(tradingSymbol)
                    .displayName(e.getDisplaySymbol())
                    .symbol(symbol)
                    .expiryDate(e.getExpiry())
                    .strikePrice(e.getStrike() == null ? null : String.valueOf(e.getStrike()))
                    .optionType(e.getOptionType())
                    .build();
            out.add(inst);
        }
        return out;
    }

    private static String deriveSymbol(String tradingSymbol, String instrumentType) {
        if (tradingSymbol == null || tradingSymbol.isBlank()) return instrumentType;
        int dash = tradingSymbol.indexOf('-');
        return dash > 0 ? tradingSymbol.substring(0, dash) : tradingSymbol;
    }

    /**
     * Re-create the {@code subscriptionGroups} map that live mode populates
     * via {@code InstrumentSearchController#quickSubscribe}. Without this,
     * the Angular {@code stocks-table} sees an empty
     * {@code /api/instruments/subscribed-groups} and silently drops every
     * equity tick because no index→stock mapping exists.
     *
     * <p>Strategy: derive parent index names from {@code basketPreset}
     * (e.g. {@code "BANKNIFTY_FULL+NIFTY50_FULL" -> [BANKNIFTY, NIFTY]}),
     * then for each parent build a state-key set containing
     * (a) the index spot, (b) derivatives whose tradingSymbol begins with
     * "{parent}-", and (c) cached constituent equities from
     * {@link MasterDataService#getIndexConstituents}, intersected with the
     * actual instruments recorded in this session.
     */
    private void seedSubscriptionGroups(SessionManifest manifest) {
        List<String> parents = parseParentIndices(manifest.getBasketPreset());
        if (parents.isEmpty() || manifest.getInstruments() == null) {
            log.warn("[REPLAY] No basket parents derived from preset='{}' — equity grouping skipped",
                    manifest.getBasketPreset());
            return;
        }

        // All securityIds (segment-qualified) actually present in the manifest.
        Set<String> manifestKeys = new HashSet<>();
        Set<String> manifestEquityIds = new HashSet<>();
        for (SessionManifest.InstrumentEntry e : manifest.getInstruments()) {
            if (e == null || e.getExchangeSegment() == null) continue;
            String sid = String.valueOf(e.getSecurityId());
            String key = e.getExchangeSegment() + ":" + sid;
            manifestKeys.add(key);
            if ("EQUITY".equalsIgnoreCase(e.getInstrumentType())) {
                manifestEquityIds.add(sid);
            }
        }

        for (String parent : parents) {
            Set<String> groupKeys = new LinkedHashSet<>();

            // (a) + (b) index + derivatives by tradingSymbol prefix
            String prefix = parent + "-";
            for (SessionManifest.InstrumentEntry e : manifest.getInstruments()) {
                if (e == null || e.getTradingSymbol() == null) continue;
                String sym = e.getTradingSymbol();
                boolean isThisIndex = sym.equalsIgnoreCase(parent)
                        && "INDEX".equalsIgnoreCase(e.getInstrumentType());
                boolean isThisDerivative = sym.regionMatches(true, 0, prefix, 0, prefix.length());
                if (isThisIndex || isThisDerivative) {
                    groupKeys.add(e.getExchangeSegment() + ":" + e.getSecurityId());
                }
            }

            // (c) constituent equities — cached CSV lookup, intersect with manifest
            try {
                List<IndexInstrument> constituents = masterData.getIndexConstituents(parent);
                int matched = 0;
                for (IndexInstrument c : constituents) {
                    if (c == null || c.getSecurityId() == null) continue;
                    if (manifestEquityIds.contains(c.getSecurityId())) {
                        groupKeys.add(c.getExchangeSegment() + ":" + c.getSecurityId());
                        matched++;
                    }
                }
                log.info("[REPLAY] Group '{}': {} derivatives + {} equity constituents matched",
                        parent, groupKeys.size() - matched, matched);
            } catch (Exception ex) {
                log.warn("[REPLAY] Could not load constituents for {}: {}", parent, ex.getMessage());
            }

            if (!groupKeys.isEmpty()) {
                stateService.registerGroup(parent, groupKeys);
            }
        }
    }

    /**
     * "BANKNIFTY_FULL+NIFTY50_FULL" → [BANKNIFTY, NIFTY].
     * Strips trailing _FULL/_QUOTE/_TICKER and normalises NIFTY50→NIFTY.
     */
    static List<String> parseParentIndices(String basketPreset) {
        List<String> out = new ArrayList<>();
        if (basketPreset == null || basketPreset.isBlank()) return out;
        for (String token : basketPreset.split("\\+")) {
            String t = token.trim().toUpperCase();
            for (String suffix : new String[]{"_FULL", "_QUOTE", "_TICKER"}) {
                if (t.endsWith(suffix)) { t = t.substring(0, t.length() - suffix.length()); break; }
            }
            if (t.equals("NIFTY50")) t = "NIFTY";
            if (!t.isEmpty() && !out.contains(t)) out.add(t);
        }
        return out;
    }
}
