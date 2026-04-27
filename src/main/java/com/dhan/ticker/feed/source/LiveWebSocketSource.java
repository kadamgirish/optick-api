package com.dhan.ticker.feed.source;

import com.dhan.ticker.exception.InvalidInstrumentException;
import com.dhan.ticker.exception.WebSocketException;
import com.dhan.ticker.feed.TickPipeline;
import com.dhan.ticker.feed.TickStateService;
import com.dhan.ticker.model.ConnectRequest;
import com.dhan.ticker.model.IndexInstrument;
import com.dhan.ticker.service.MasterDataService;
import jakarta.annotation.PreDestroy;
import lombok.extern.slf4j.Slf4j;
import org.java_websocket.client.WebSocketClient;
import org.java_websocket.handshake.ServerHandshake;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.net.URI;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

/**
 * Live Dhan WebSocket source. Owns the {@link WebSocketClient} lifecycle and
 * the subscribe-payload builder; pushes every binary frame straight into the
 * shared {@link TickPipeline}. Stateless w.r.t. ticks — the pipeline +
 * {@link TickStateService} handle decoding, enrichment, and broadcast.
 */
@Slf4j
@Component
public class LiveWebSocketSource implements FrameSource {

    @Value("${dhan.websocket-url}")
    private String websocketUrl;

    @Value("${dhan.access-token}")
    private String accessToken;

    @Value("${dhan.client-id}")
    private String clientId;

    private final MasterDataService masterDataService;
    private final TickStateService stateService;
    private final TickPipeline pipeline;

    private final AtomicReference<WebSocketClient> wsClientRef = new AtomicReference<>();
    private final AtomicBoolean connected = new AtomicBoolean(false);
    private final AtomicLong lastTickTime = new AtomicLong(0);

    private final ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
        Thread t = new Thread(r, "live-ws-watchdog");
        t.setDaemon(true);
        return t;
    });
    private ScheduledFuture<?> noDataWatcher;

    private static final Map<String, Integer> FEED_MODE_REQUEST_CODE = Map.of(
            "TICKER", 15,
            "QUOTE", 17,
            "FULL", 21
    );

    public LiveWebSocketSource(MasterDataService masterDataService,
                               TickStateService stateService,
                               TickPipeline pipeline) {
        this.masterDataService = masterDataService;
        this.stateService = stateService;
        this.pipeline = pipeline;
        // Live source listens for server-initiated disconnect packets so it can
        // flip its own connected flag without leaking WS knowledge into the state svc.
        this.stateService.setDisconnectListener(d -> connected.set(false));
    }

    // ── Public lifecycle (called by MarketFeedService façade) ─────────────

    public List<String> connect(List<ConnectRequest.InstrumentSub> instruments, String defaultFeedMode) {
        if (connected.get()) {
            return subscribe(instruments, defaultFeedMode);
        }

        List<ResolvedSub> resolved = resolveInstruments(instruments, defaultFeedMode);
        if (resolved.isEmpty()) {
            throw new InvalidInstrumentException("No valid instruments to subscribe");
        }

        pipeline.resume();
        lastTickTime.set(System.currentTimeMillis());

        String url = String.format("%s?version=2&token=%s&clientId=%s&authType=2",
                websocketUrl, accessToken, clientId);

        List<String> subscribed = new ArrayList<>();
        try {
            WebSocketClient client = new WebSocketClient(URI.create(url)) {
                @Override
                public void onOpen(ServerHandshake handshake) {
                    log.info("WebSocket connected. HTTP status: {}", handshake.getHttpStatus());
                    connected.set(true);
                    subscribeAll(resolved);
                    startNoDataWatcher();
                }

                @Override
                public void onMessage(String message) {
                    log.debug("Text message: {}", message);
                }

                @Override
                public void onMessage(ByteBuffer bytes) {
                    lastTickTime.set(System.currentTimeMillis());
                    pipeline.accept(bytes);
                }

                @Override
                public void onClose(int code, String reason, boolean remote) {
                    connected.set(false);
                    stopNoDataWatcher();
                    log.warn("WebSocket closed. Code: {}, Reason: {}, Remote: {}", code, reason, remote);
                }

                @Override
                public void onError(Exception ex) {
                    log.error("WebSocket error: {}", ex.getMessage(), ex);
                }
            };

            client.setConnectionLostTimeout(30);
            client.connect();
            wsClientRef.set(client);

            for (ResolvedSub r : resolved) {
                stateService.registerSubscribed(r.instrument);
                subscribed.add(r.instrument.getSymbol() + "(" + r.instrument.getSecurityId() + ")");
            }
            log.info("Connecting to Dhan WebSocket. Instruments: {}", subscribed);
        } catch (Exception e) {
            throw new WebSocketException("Failed to connect: " + e.getMessage(), e);
        }
        return subscribed;
    }

    public List<String> subscribe(List<ConnectRequest.InstrumentSub> instruments, String defaultFeedMode) {
        if (!connected.get()) {
            throw new WebSocketException("WebSocket is not connected. Call /api/ws/connect first.");
        }
        List<ResolvedSub> resolved = resolveInstruments(instruments, defaultFeedMode);
        if (resolved.isEmpty()) {
            throw new InvalidInstrumentException("No valid instruments to subscribe");
        }

        subscribeAll(resolved);
        List<String> added = new ArrayList<>();
        for (ResolvedSub r : resolved) {
            stateService.registerSubscribed(r.instrument);
            added.add(r.instrument.getSymbol() + "(" + r.instrument.getSecurityId() + ")");
        }
        return added;
    }

    public List<String> unsubscribe(List<String> securityIds) {
        if (!connected.get()) {
            throw new WebSocketException("WebSocket is not connected");
        }
        List<String> removed = stateService.unsubscribe(securityIds);
        log.info("Unsubscribed: {}", removed);
        return removed;
    }

    public void disconnect() {
        stopNoDataWatcher();
        WebSocketClient client = wsClientRef.getAndSet(null);
        if (client != null && !client.isClosed()) {
            try {
                client.send("{\"RequestCode\": 12}");
                client.close();
                log.info("WebSocket disconnected");
            } catch (Exception e) {
                log.warn("Error during disconnect: {}", e.getMessage());
            }
        }
        connected.set(false);
        pipeline.resume();
        stateService.clearAllSubscriptions();
    }

    public boolean isConnected() { return connected.get(); }

    @Override
    public boolean isOpen() { return connected.get(); }

    @Override
    public void stop() { disconnect(); }

    @PreDestroy
    public void shutdown() {
        try { stop(); } catch (Exception ignored) {}
        scheduler.shutdownNow();
    }

    // ── Internal: resolve & send subscribe payload ───────────────────────

    private record ResolvedSub(IndexInstrument instrument, String feedMode) {}

    private List<ResolvedSub> resolveInstruments(List<ConnectRequest.InstrumentSub> subs, String defaultMode) {
        List<ResolvedSub> resolved = new ArrayList<>();
        for (ConnectRequest.InstrumentSub sub : subs) {
            IndexInstrument inst;
            if (sub.getExchangeSegment() != null && !sub.getExchangeSegment().isBlank()) {
                inst = masterDataService.findBySecurityId(sub.getExchangeSegment(), sub.getSecurityId())
                        .orElse(masterDataService.findBySecurityId(sub.getSecurityId()).orElse(null));
            } else {
                inst = masterDataService.findBySecurityId(sub.getSecurityId()).orElse(null);
            }
            if (inst == null) {
                log.warn("Skipping unknown securityId: {}", sub.getSecurityId());
                continue;
            }
            if (sub.getExchangeSegment() != null && !sub.getExchangeSegment().isBlank()) {
                inst.setExchangeSegment(sub.getExchangeSegment());
            }
            String mode = sub.getFeedMode() != null ? sub.getFeedMode().toUpperCase() : defaultMode.toUpperCase();
            if ("IDX_I".equalsIgnoreCase(inst.getExchangeSegment()) && !"TICKER".equals(mode)) {
                log.info("INDEX {} only supports TICKER. Overriding {} → TICKER", inst.getSymbol(), mode);
                mode = "TICKER";
            }
            if (!FEED_MODE_REQUEST_CODE.containsKey(mode)) {
                mode = "TICKER";
            }
            resolved.add(new ResolvedSub(inst, mode));
        }
        return resolved;
    }

    private void subscribeAll(List<ResolvedSub> resolved) {
        Map<String, List<ResolvedSub>> byMode = resolved.stream()
                .collect(Collectors.groupingBy(ResolvedSub::feedMode));

        WebSocketClient client = wsClientRef.get();
        if (client == null || !client.isOpen()) return;

        for (Map.Entry<String, List<ResolvedSub>> entry : byMode.entrySet()) {
            String mode = entry.getKey();
            List<ResolvedSub> group = entry.getValue();
            int requestCode = FEED_MODE_REQUEST_CODE.get(mode);

            StringBuilder instrumentList = new StringBuilder();
            for (int i = 0; i < group.size(); i++) {
                ResolvedSub r = group.get(i);
                if (i > 0) instrumentList.append(",");
                instrumentList.append(String.format("""
                        {"ExchangeSegment":"%s","SecurityId":"%s"}""",
                        r.instrument.getExchangeSegment(), r.instrument.getSecurityId()));
            }

            String msg = String.format("""
                    {"RequestCode":%d,"InstrumentCount":%d,"InstrumentList":[%s]}""",
                    requestCode, group.size(), instrumentList);

            client.send(msg);
            log.info("Subscribed [{}] RequestCode={} for {} instruments: {}", mode, requestCode,
                    group.size(), group.stream()
                            .map(r -> r.instrument.getSymbol() + "(" + r.instrument.getSecurityId() + ")")
                            .collect(Collectors.joining(", ")));
        }
    }

    private void startNoDataWatcher() {
        stopNoDataWatcher();
        noDataWatcher = scheduler.scheduleAtFixedRate(() -> {
            if (connected.get() && (System.currentTimeMillis() - lastTickTime.get()) > 30_000) {
                log.warn("[WARN] No data received for 30s — market may be closed or instruments inactive");
            }
        }, 30, 30, TimeUnit.SECONDS);
    }

    private void stopNoDataWatcher() {
        if (noDataWatcher != null) {
            noDataWatcher.cancel(false);
            noDataWatcher = null;
        }
    }
}
