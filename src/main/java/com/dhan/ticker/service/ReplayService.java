package com.dhan.ticker.service;

import com.dhan.ticker.exception.WebSocketException;
import com.dhan.ticker.model.ConnectRequest;
import com.dhan.ticker.model.DatasetSummary;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Loads recorded WebSocket sessions from disk and replays binary frames
 * into MarketFeedService.ingestFrame(). Mutually exclusive with live feed.
 *
 * On-disk format per session folder:
 *   - manifest.json      session metadata (startTime, instruments, etc.)
 *   - frames.bin         raw concatenated Dhan binary WebSocket frames
 *   - frames.idx.ndjson  one JSON per line: {timestampNanos, offset, length, frameType}
 */
@Slf4j
@Service
public class ReplayService {

    @Value("${dhan.replay.data-dir:/Users/kadamgirish/Desktop/DATA}")
    private String dataDir;

    private final MarketFeedService feedService;
    private final ObjectMapper mapper = new ObjectMapper();

    private final AtomicReference<ReplaySession> sessionRef = new AtomicReference<>();

    public ReplayService(MarketFeedService feedService) {
        this.feedService = feedService;
    }

    // ── Dataset discovery ─────────────────────────────────────────────────

    public List<DatasetSummary> listDatasets() {
        File root = new File(dataDir);
        if (!root.isDirectory()) {
            log.warn("Replay data-dir does not exist: {}", dataDir);
            return List.of();
        }
        File[] folders = root.listFiles(f -> f.isDirectory() && !f.getName().startsWith("."));
        if (folders == null) return List.of();

        List<DatasetSummary> out = new ArrayList<>();
        for (File f : folders) {
            File manifest = new File(f, "manifest.json");
            File framesBin = new File(f, "frames.bin");
            File idx = new File(f, "frames.idx.ndjson");
            if (!manifest.isFile() || !framesBin.isFile() || !idx.isFile()) {
                log.debug("Skipping incomplete session folder: {}", f.getName());
                continue;
            }
            try {
                JsonNode m = mapper.readTree(manifest);
                String start = m.path("startTime").asText(null);
                String end = m.path("endTime").asText(null);
                long duration = 0;
                if (start != null && end != null) {
                    try {
                        duration = Duration.between(Instant.parse(start), Instant.parse(end)).getSeconds();
                    } catch (Exception ignore) {}
                }
                int instrumentCount = m.path("instruments").isArray() ? m.path("instruments").size() : 0;
                out.add(DatasetSummary.builder()
                        .sessionId(m.path("sessionId").asText(f.getName()))
                        .basketPreset(m.path("basketPreset").asText(null))
                        .feedMode(m.path("feedMode").asText(null))
                        .startTime(toIst(start))
                        .endTime(toIst(end))
                        .durationSeconds(duration)
                        .instrumentCount(instrumentCount)
                        .framesBinSizeBytes(framesBin.length())
                        .indexFileSizeBytes(idx.length())
                        .build());
            } catch (IOException e) {
                log.warn("Failed to read manifest in {}: {}", f.getName(), e.getMessage());
            }
        }
        out.sort(Comparator.comparing(DatasetSummary::getSessionId));
        return out;
    }

    // ── Load / start / control ────────────────────────────────────────────

    public synchronized Map<String, Object> load(String sessionId) {
        if (feedService.getFeedSource() == MarketFeedService.FeedSource.LIVE
                && feedService.isConnected()) {
            throw new WebSocketException("Live WebSocket is connected. Disconnect before loading replay.");
        }
        stop(); // any prior session

        Path folder = resolveFolder(sessionId);
        Path framesPath = folder.resolve("frames.bin");
        Path idxPath = folder.resolve("frames.idx.ndjson");
        Path manifestPath = folder.resolve("manifest.json");
        if (!Files.isRegularFile(framesPath) || !Files.isRegularFile(idxPath) || !Files.isRegularFile(manifestPath)) {
            throw new IllegalArgumentException("Session folder missing required files: " + sessionId);
        }

        long[] tsNanos;
        long[] offsets;
        int[] lengths;
        Instant sessionStart;
        JsonNode manifestInstruments;
        try {
            JsonNode manifest = mapper.readTree(manifestPath.toFile());
            String startStr = manifest.path("startTime").asText();
            sessionStart = Instant.parse(startStr);
            manifestInstruments = manifest.path("instruments");

            log.info("[REPLAY] Loading index: {} (size={} MB)",
                    idxPath, idxPath.toFile().length() / 1024 / 1024);
            int estimated = (int) Math.min(Integer.MAX_VALUE - 8, idxPath.toFile().length() / 70);
            long[] t = new long[Math.max(estimated, 1024)];
            long[] o = new long[t.length];
            int[] l = new int[t.length];
            int n = 0;

            try (BufferedReader r = new BufferedReader(new FileReader(idxPath.toFile()), 1 << 20)) {
                String line;
                while ((line = r.readLine()) != null) {
                    // Fast manual parse — format is rigid:
                    // {"timestampNanos":N,"offset":N,"length":N,"frameType":"binary"}
                    long ts = extractLong(line, "\"timestampNanos\":");
                    long off = extractLong(line, "\"offset\":");
                    int len = (int) extractLong(line, "\"length\":");
                    // skip non-binary frames (if ever present)
                    if (line.indexOf("\"frameType\":\"binary\"") < 0
                            && line.indexOf("\"frameType\": \"binary\"") < 0) {
                        continue;
                    }
                    if (n == t.length) {
                        int cap = Math.toIntExact(Math.min((long) t.length * 2L, Integer.MAX_VALUE - 8));
                        t = Arrays.copyOf(t, cap);
                        o = Arrays.copyOf(o, cap);
                        l = Arrays.copyOf(l, cap);
                    }
                    t[n] = ts;
                    o[n] = off;
                    l[n] = len;
                    n++;
                }
            }

            tsNanos = Arrays.copyOf(t, n);
            offsets = Arrays.copyOf(o, n);
            lengths = Arrays.copyOf(l, n);
            log.info("[REPLAY] Indexed {} frames for session {}", n, sessionId);
        } catch (IOException e) {
            throw new RuntimeException("Failed to load session " + sessionId + ": " + e.getMessage(), e);
        }

        ReplaySession s;
        try {
            s = new ReplaySession(sessionId, framesPath, tsNanos, offsets, lengths, sessionStart, manifestInstruments);
        } catch (IOException e) {
            throw new RuntimeException("Failed to open frames.bin: " + e.getMessage(), e);
        }
        sessionRef.set(s);
        feedService.setFeedSource(MarketFeedService.FeedSource.REPLAY);
        feedService.clearSubscriptionState();

        Map<String, Object> info = new LinkedHashMap<>();
        info.put("sessionId", sessionId);
        info.put("totalFrames", tsNanos.length);
        info.put("sessionStart", toIst(sessionStart.toString()));
        info.put("durationSeconds", tsNanos.length > 0
                ? Duration.ofNanos(tsNanos[tsNanos.length - 1] - tsNanos[0]).getSeconds() : 0);
        return info;
    }

    public synchronized void start() {
        ReplaySession s = sessionRef.get();
        if (s == null) throw new IllegalStateException("No session loaded. POST /api/replay/load first.");
        if (s.playing.get()) {
            s.paused.set(false);
            return;
        }
        s.playing.set(true);
        s.paused.set(false);
        Thread t = new Thread(() -> playbackLoop(s), "replay-" + s.sessionId);
        t.setDaemon(true);
        s.threadRef.set(t);
        t.start();
    }

    public void pause() {
        ReplaySession s = sessionRef.get();
        if (s == null) throw new IllegalStateException("No session loaded");
        s.paused.set(true);
    }

    public void resume() {
        ReplaySession s = sessionRef.get();
        if (s == null) throw new IllegalStateException("No session loaded");
        synchronized (s.pauseLock) {
            s.paused.set(false);
            s.pauseLock.notifyAll();
        }
    }

    public void setSpeed(double value) {
        if (value <= 0 || Double.isNaN(value) || Double.isInfinite(value)) {
            throw new IllegalArgumentException("speed must be > 0");
        }
        ReplaySession s = sessionRef.get();
        if (s == null) throw new IllegalStateException("No session loaded");
        s.speed.set(value);
    }

    public void seek(String isoTime) {
        ReplaySession s = sessionRef.get();
        if (s == null) throw new IllegalStateException("No session loaded");
        Instant target;
        try {
            target = Instant.parse(isoTime);
        } catch (Exception e) {
            // Accept IST-formatted input like 2026-04-17T09:00:38+05:30 or 2026-04-17T09:00:38
            try {
                target = java.time.OffsetDateTime.parse(isoTime).toInstant();
            } catch (Exception e2) {
                target = java.time.LocalDateTime.parse(isoTime).atZone(IST).toInstant();
            }
        }
        long offsetNanos = Duration.between(s.sessionStart, target).toNanos();
        long targetNanoVal = s.tsNanos[0] + offsetNanos;
        int idx = lowerBound(s.tsNanos, targetNanoVal);
        s.cursor.set(idx);
        log.info("[REPLAY] Seek to {} → frame index {}", isoTime, idx);
    }

    public synchronized void stop() {
        ReplaySession s = sessionRef.getAndSet(null);
        if (s != null) {
            s.playing.set(false);
            synchronized (s.pauseLock) {
                s.paused.set(false);
                s.pauseLock.notifyAll();
            }
            try { if (s.raf != null) s.raf.close(); } catch (IOException ignore) {}
            Thread t = s.threadRef.get();
            if (t != null) t.interrupt();
        }
        feedService.clearSubscriptionState();
        if (feedService.getFeedSource() == MarketFeedService.FeedSource.REPLAY) {
            feedService.setFeedSource(MarketFeedService.FeedSource.NONE);
        }
    }

    public Map<String, Object> status() {
        Map<String, Object> m = new LinkedHashMap<>();
        ReplaySession s = sessionRef.get();
        if (s == null) {
            m.put("loaded", false);
            return m;
        }
        int cur = s.cursor.get();
        int total = s.tsNanos.length;
        long curNano = cur < total ? s.tsNanos[cur] : (total > 0 ? s.tsNanos[total - 1] : 0L);
        Instant curWall = total > 0
                ? s.sessionStart.plusNanos(curNano - s.tsNanos[0])
                : s.sessionStart;
        m.put("loaded", true);
        m.put("sessionId", s.sessionId);
        m.put("playing", s.playing.get());
        m.put("paused", s.paused.get());
        m.put("speed", s.speed.get());
        m.put("cursor", cur);
        m.put("totalFrames", total);
        m.put("progressPct", total > 0 ? (100.0 * cur / total) : 0.0);
        m.put("sessionStart", toIst(s.sessionStart.toString()));
        m.put("currentTime", toIst(curWall.toString()));
        return m;
    }

    /**
     * Build subscribe requests for a specific index from the currently-loaded
     * session's manifest.instruments[]. This matters because recorded securityIds
     * (future expiry, option strikes) no longer match today's live chain.
     *
     * Filters by tag+tradingSymbol prefix:
     *   - INDEX        → tag="INDEX" and tradingSymbol==indexSymbol
     *   - FUTURE       → tag="FUTURE" and tradingSymbol startsWith indexSymbol+"-"
     *   - OPTIONS      → tag starts with "CE_"/"PE_" and tradingSymbol startsWith indexSymbol+"-"
     *
     * Spot gets feedMode=TICKER (IDX_I constraint); everything else uses default feedMode.
     * Returns empty list if no matches or no session loaded.
     */
    public List<ConnectRequest.InstrumentSub> getSubscribeRequestsForIndex(String indexSymbol) {
        ReplaySession s = sessionRef.get();
        if (s == null || s.manifestInstruments == null || !s.manifestInstruments.isArray()) {
            return List.of();
        }
        String symUpper = indexSymbol.toUpperCase();
        String prefix = symUpper + "-";
        List<ConnectRequest.InstrumentSub> out = new ArrayList<>();
        for (JsonNode inst : s.manifestInstruments) {
            String tag = inst.path("tag").asText("");
            String tradingSymbol = inst.path("tradingSymbol").asText("");
            String securityId = inst.path("securityId").asText(null);
            String exchangeSegment = inst.path("exchangeSegment").asText(null);
            if (securityId == null || exchangeSegment == null) continue;

            boolean matches = false;
            boolean isIndex = false;
            if ("INDEX".equals(tag)) {
                // INDEX tag doesn't carry symbol suffix; match on tradingSymbol
                if (symUpper.equalsIgnoreCase(tradingSymbol)) {
                    matches = true;
                    isIndex = true;
                }
            } else if ("FUTURE".equals(tag) && tradingSymbol.toUpperCase().startsWith(prefix)) {
                matches = true;
            } else if ((tag.startsWith("CE_") || tag.startsWith("PE_"))
                    && tradingSymbol.toUpperCase().startsWith(prefix)) {
                matches = true;
            }
            if (!matches) continue;

            ConnectRequest.InstrumentSub sub = new ConnectRequest.InstrumentSub();
            sub.setSecurityId(securityId);
            sub.setExchangeSegment(exchangeSegment);
            if (isIndex) sub.setFeedMode("TICKER");
            out.add(sub);
        }
        log.info("[REPLAY] Built {} subscribe requests for index {} from manifest", out.size(), symUpper);
        return out;
    }

    // ── Playback loop ─────────────────────────────────────────────────────

    private void playbackLoop(ReplaySession s) {
        byte[] buf = new byte[64 * 1024];
        try {
            while (s.playing.get() && s.cursor.get() < s.tsNanos.length) {
                // Wait while paused
                while (s.paused.get() && s.playing.get()) {
                    synchronized (s.pauseLock) {
                        try { s.pauseLock.wait(200); } catch (InterruptedException e) { return; }
                    }
                }
                if (!s.playing.get()) break;

                int i = s.cursor.get();
                int len = s.lengths[i];
                long off = s.offsets[i];
                if (buf.length < len) buf = new byte[Math.max(len, buf.length * 2)];

                synchronized (s.raf) {
                    s.raf.seek(off);
                    s.raf.readFully(buf, 0, len);
                }
                ByteBuffer bb = ByteBuffer.wrap(buf, 0, len).order(ByteOrder.LITTLE_ENDIAN);
                feedService.ingestFrame(bb);

                s.cursor.incrementAndGet();

                // Sleep by inter-frame gap / speed
                if (i + 1 < s.tsNanos.length) {
                    long gapNanos = s.tsNanos[i + 1] - s.tsNanos[i];
                    double speed = s.speed.get();
                    long sleepNanos = (long) (gapNanos / Math.max(speed, 1e-9));
                    if (sleepNanos > 10_000) {
                        try {
                            long ms = sleepNanos / 1_000_000L;
                            int ns = (int) (sleepNanos % 1_000_000L);
                            Thread.sleep(ms, ns);
                        } catch (InterruptedException e) {
                            return;
                        }
                    }
                }
            }
            log.info("[REPLAY] Playback finished (cursor={}, total={})",
                    s.cursor.get(), s.tsNanos.length);
            s.playing.set(false);
        } catch (Exception e) {
            log.error("[REPLAY] Playback error: {}", e.getMessage(), e);
            s.playing.set(false);
        }
    }

    // ── Helpers ───────────────────────────────────────────────────────────

    private static final ZoneId IST = ZoneId.of("Asia/Kolkata");
    private static final DateTimeFormatter IST_FMT =
            DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSSXXX").withZone(IST);

    /** Format an ISO-8601 UTC instant string as IST (Asia/Kolkata). Returns null on null/invalid input. */
    static String toIst(String isoUtc) {
        if (isoUtc == null || isoUtc.isBlank()) return null;
        try {
            return IST_FMT.format(Instant.parse(isoUtc));
        } catch (Exception e) {
            return isoUtc;
        }
    }

    private Path resolveFolder(String sessionId) {
        Path p = Paths.get(dataDir, sessionId);
        if (!Files.isDirectory(p)) {
            throw new IllegalArgumentException("Unknown sessionId: " + sessionId);
        }
        return p;
    }

    /** Returns the index of the first element >= target, or length if all smaller. */
    private static int lowerBound(long[] arr, long target) {
        int lo = 0, hi = arr.length;
        while (lo < hi) {
            int mid = (lo + hi) >>> 1;
            if (arr[mid] < target) lo = mid + 1;
            else hi = mid;
        }
        return lo;
    }

    /** Extract a long integer value immediately following a literal key like "\"offset\":". */
    private static long extractLong(String line, String key) {
        int k = line.indexOf(key);
        if (k < 0) return 0L;
        int p = k + key.length();
        // skip whitespace
        while (p < line.length() && line.charAt(p) == ' ') p++;
        long val = 0;
        boolean neg = false;
        if (p < line.length() && line.charAt(p) == '-') { neg = true; p++; }
        while (p < line.length()) {
            char c = line.charAt(p);
            if (c < '0' || c > '9') break;
            val = val * 10 + (c - '0');
            p++;
        }
        return neg ? -val : val;
    }

    // ── Session state ─────────────────────────────────────────────────────

    private static final class ReplaySession {
        final String sessionId;
        final long[] tsNanos;
        final long[] offsets;
        final int[] lengths;
        final Instant sessionStart;
        final RandomAccessFile raf;
        final JsonNode manifestInstruments;

        final AtomicInteger cursor = new AtomicInteger(0);
        final AtomicBoolean playing = new AtomicBoolean(false);
        final AtomicBoolean paused = new AtomicBoolean(false);
        final AtomicReference<Double> speed = new AtomicReference<>(1.0);
        final Object pauseLock = new Object();
        final AtomicReference<Thread> threadRef = new AtomicReference<>();

        ReplaySession(String sessionId, Path framesBin,
                      long[] tsNanos, long[] offsets, int[] lengths,
                      Instant sessionStart, JsonNode manifestInstruments) throws IOException {
            this.sessionId = sessionId;
            this.tsNanos = tsNanos;
            this.offsets = offsets;
            this.lengths = lengths;
            this.sessionStart = sessionStart;
            this.manifestInstruments = manifestInstruments;
            this.raf = new RandomAccessFile(framesBin.toFile(), "r");
        }
    }
}
