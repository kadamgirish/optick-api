package com.dhan.ticker.service;

import com.dhan.ticker.exception.InvalidInstrumentException;
import com.dhan.ticker.exception.WebSocketException;
import com.dhan.ticker.model.ConnectRequest;
import com.dhan.ticker.model.IndexInstrument;
import lombok.extern.slf4j.Slf4j;
import org.java_websocket.client.WebSocketClient;
import org.java_websocket.handshake.ServerHandshake;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.net.URI;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

@Slf4j
@Service
public class DhanWebSocketService {

    @Value("${dhan.websocket-url}")
    private String websocketUrl;

    @Value("${dhan.access-token}")
    private String accessToken;

    @Value("${dhan.client-id}")
    private String clientId;

    private final DhanMasterDataService masterDataService;

    private final AtomicReference<WebSocketClient> wsClientRef = new AtomicReference<>();
    private final AtomicBoolean paused = new AtomicBoolean(false);
    private final AtomicBoolean connected = new AtomicBoolean(false);
    private final AtomicLong lastTickTime = new AtomicLong(0);

    // securityId -> instrument metadata for all subscribed instruments
    private final Map<String, IndexInstrument> subscribedInstruments = new ConcurrentHashMap<>();

    private final ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();
    private ScheduledFuture<?> noDataWatcher;

    private static final DateTimeFormatter TIME_FMT = DateTimeFormatter
            .ofPattern("yyyy-MM-dd HH:mm:ss")
            .withZone(ZoneId.of("Asia/Kolkata"));

    private static final Map<Integer, String> EXCHANGE_SEGMENT_NAMES = Map.of(
            0, "IDX_I",
            1, "NSE_EQ",
            2, "NSE_FNO",
            3, "NSE_CURRENCY",
            4, "BSE_EQ",
            5, "MCX_COMM",
            7, "BSE_CURRENCY",
            8, "BSE_FNO"
    );

    private static final Map<String, Integer> FEED_MODE_REQUEST_CODE = Map.of(
            "TICKER", 15,
            "QUOTE", 17,
            "FULL", 21
    );

    public DhanWebSocketService(DhanMasterDataService masterDataService) {
        this.masterDataService = masterDataService;
    }

    // ── Connect & subscribe to multiple instruments ───────────────────────

    public List<String> connect(List<ConnectRequest.InstrumentSub> instruments, String defaultFeedMode) {
        if (connected.get()) {
            disconnect();
        }

        // Resolve all instruments first (fail fast for invalid ones)
        List<ResolvedSub> resolved = resolveInstruments(instruments, defaultFeedMode);
        if (resolved.isEmpty()) {
            throw new InvalidInstrumentException("No valid instruments to subscribe");
        }

        paused.set(false);
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
                    if (!paused.get()) {
                        handleBinaryMessage(bytes);
                    }
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
                subscribedInstruments.put(r.instrument.getSecurityId(), r.instrument);
                subscribed.add(r.instrument.getSymbol() + "(" + r.instrument.getSecurityId() + ")");
            }

            log.info("Connecting to Dhan WebSocket. Instruments: {}", subscribed);
        } catch (Exception e) {
            throw new WebSocketException("Failed to connect: " + e.getMessage(), e);
        }

        return subscribed;
    }

    // ── Add instruments to an existing connection ─────────────────────────

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
            subscribedInstruments.put(r.instrument.getSecurityId(), r.instrument);
            added.add(r.instrument.getSymbol() + "(" + r.instrument.getSecurityId() + ")");
        }
        return added;
    }

    // ── Unsubscribe specific instruments ──────────────────────────────────

    public List<String> unsubscribe(List<String> securityIds) {
        if (!connected.get()) {
            throw new WebSocketException("WebSocket is not connected");
        }
        List<String> removed = new ArrayList<>();
        for (String sid : securityIds) {
            IndexInstrument inst = subscribedInstruments.remove(sid);
            if (inst != null) {
                removed.add(inst.getSymbol() + "(" + sid + ")");
            }
        }
        // Dhan doesn't have per-instrument unsubscribe for ticker mode (16),
        // but we stop logging by removing from the map
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
        paused.set(false);
        subscribedInstruments.clear();
    }

    public void pause() {
        if (!connected.get()) throw new WebSocketException("WebSocket is not connected");
        paused.set(true);
        log.info("Tick logging paused");
    }

    public void resume() {
        if (!connected.get()) throw new WebSocketException("WebSocket is not connected");
        paused.set(false);
        log.info("Tick logging resumed");
    }

    public boolean isConnected() { return connected.get(); }
    public boolean isPaused() { return paused.get(); }

    public List<IndexInstrument> getSubscribedInstruments() {
        return new ArrayList<>(subscribedInstruments.values());
    }

    // ── Internal: resolve & group instruments, then send subscribe msgs ───

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
            // IDX_I only supports TICKER
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
        // Group by effective feedMode, then send one subscribe JSON per mode
        // (Dhan allows up to 100 instruments per message)
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

    // ── No-data watcher ───────────────────────────────────────────────────

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

    // ── Binary message dispatcher ─────────────────────────────────────────

    private void handleBinaryMessage(ByteBuffer buffer) {
        try {
            buffer.order(ByteOrder.LITTLE_ENDIAN);
            if (buffer.remaining() < 8) return;

            int responseCode = Byte.toUnsignedInt(buffer.get(0));
            int exchangeSegmentCode = Byte.toUnsignedInt(buffer.get(3));
            int securityId = buffer.getInt(4);

            String exchangeSegment = EXCHANGE_SEGMENT_NAMES.getOrDefault(exchangeSegmentCode,
                    "UNKNOWN(" + exchangeSegmentCode + ")");

            // Look up instrument from our subscribed map by securityId
            IndexInstrument inst = subscribedInstruments.get(String.valueOf(securityId));
            String symbol = inst != null ? inst.getSymbol() : (exchangeSegment + ":" + securityId);
            String type = detectType(inst);

            switch (responseCode) {
                case 1  -> handleTickerPacket(buffer, symbol, type);
                case 2  -> handleTickerPacket(buffer, symbol, type);
                case 4  -> handleQuotePacket(buffer, symbol, type);
                case 5  -> handleOIPacket(buffer, symbol);
                case 6  -> handlePrevClosePacket(buffer, symbol);
                case 7  -> log.info("[MARKET_STATUS] Exchange: {}, SecurityId: {}", exchangeSegment, securityId);
                case 8  -> handleFullPacket(buffer, symbol, type);
                case 50 -> handleDisconnectPacket(buffer);
                default -> log.debug("Unknown response code {} securityId: {}", responseCode, securityId);
            }
        } catch (Exception e) {
            log.error("Error parsing binary message: {}", e.getMessage(), e);
        }
    }

    // ── Ticker Packet (code 1=Index, 2=Ticker) ───────────────────────────
    private void handleTickerPacket(ByteBuffer buf, String symbol, String type) {
        if (buf.remaining() < 16) return;
        float ltp = buf.getFloat(8);
        int ltt = buf.getInt(12);
        log.info("[TICK] SYMBOL={}, TYPE={}, LTP={}, TIME={}",
                symbol, type, fmt(ltp), fmtTime(ltt));
    }

    // ── Quote Packet (code 4) ─────────────────────────────────────────────
    private void handleQuotePacket(ByteBuffer buf, String symbol, String type) {
        if (buf.remaining() < 50) return;
        float ltp   = buf.getFloat(8);
        short ltq   = buf.getShort(12);
        int   ltt   = buf.getInt(14);
        float atp   = buf.getFloat(18);
        int   vol   = buf.getInt(22);
        int   sellQ = buf.getInt(26);
        int   buyQ  = buf.getInt(30);
        float open  = buf.getFloat(34);
        float close = buf.getFloat(38);
        float high  = buf.getFloat(42);
        float low   = buf.getFloat(46);
        log.info("[TICK] SYMBOL={}, TYPE={}, LTP={}, OPEN={}, HIGH={}, LOW={}, CLOSE={}, " +
                        "VOL={}, ATP={}, BUY_QTY={}, SELL_QTY={}, LTQ={}, TIME={}",
                symbol, type, fmt(ltp), fmt(open), fmt(high), fmt(low), fmt(close),
                vol, fmt(atp), buyQ, sellQ, ltq, fmtTime(ltt));
    }

    // ── Full Packet (code 8) ──────────────────────────────────────────────
    private void handleFullPacket(ByteBuffer buf, String symbol, String type) {
        if (buf.remaining() < 62) return;
        float ltp   = buf.getFloat(8);
        short ltq   = buf.getShort(12);
        int   ltt   = buf.getInt(14);
        float atp   = buf.getFloat(18);
        int   vol   = buf.getInt(22);
        int   sellQ = buf.getInt(26);
        int   buyQ  = buf.getInt(30);
        int   oi    = buf.getInt(34);
        float open  = buf.getFloat(46);
        float close = buf.getFloat(50);
        float high  = buf.getFloat(54);
        float low   = buf.getFloat(58);

        StringBuilder sb = new StringBuilder();
        sb.append(String.format("[TICK] SYMBOL=%s, TYPE=%s, LTP=%s, OPEN=%s, HIGH=%s, LOW=%s, CLOSE=%s, " +
                        "VOL=%d, ATP=%s, BUY_QTY=%d, SELL_QTY=%d, LTQ=%d",
                symbol, type, fmt(ltp), fmt(open), fmt(high), fmt(low), fmt(close),
                vol, fmt(atp), buyQ, sellQ, ltq));
        if (oi > 0) sb.append(String.format(", OI=%d", oi));
        if (buf.remaining() >= 162) {
            sb.append(" | DEPTH:");
            for (int i = 0; i < 5; i++) {
                int base = 62 + (i * 20);
                int bidQty  = buf.getInt(base);
                int askQty  = buf.getInt(base + 4);
                float bidPx = buf.getFloat(base + 12);
                float askPx = buf.getFloat(base + 16);
                sb.append(String.format(" [L%d B:%s(%d) A:%s(%d)]",
                        i + 1, fmt(bidPx), bidQty, fmt(askPx), askQty));
            }
        }
        sb.append(String.format(", TIME=%s", fmtTime(ltt)));
        log.info(sb.toString());
    }

    private void handleOIPacket(ByteBuffer buf, String symbol) {
        if (buf.remaining() < 12) return;
        log.info("[OI] SYMBOL={}, OI={}", symbol, buf.getInt(8));
    }

    private void handlePrevClosePacket(ByteBuffer buf, String symbol) {
        if (buf.remaining() < 16) return;
        log.info("[PREV_CLOSE] SYMBOL={}, PREV_CLOSE={}, PREV_OI={}",
                symbol, fmt(buf.getFloat(8)), buf.getInt(12));
    }

    private void handleDisconnectPacket(ByteBuffer buf) {
        if (buf.remaining() < 10) return;
        short code = buf.getShort(8);
        log.error("[DISCONNECT] Server disconnected. Code: {}", code);
        connected.set(false);
        stopNoDataWatcher();
    }

    // ── Helpers ───────────────────────────────────────────────────────────

    private String detectType(IndexInstrument inst) {
        if (inst == null) return "UNKNOWN";
        return switch (inst.getInstrumentType().toUpperCase()) {
            case "INDEX" -> "INDEX";
            case "FUTIDX", "FUTSTK", "FUTCOM", "FUTCUR" -> "FUTURE";
            case "OPTIDX", "OPTSTK", "OPTFUT", "OPTCUR" -> "OPTION";
            case "EQUITY" -> "EQUITY";
            default -> inst.getInstrumentType();
        };
    }

    private String fmt(float v) { return String.format("%.2f", v); }

    private String fmtTime(int epoch) {
        return epoch > 0 ? TIME_FMT.format(Instant.ofEpochSecond(epoch)) : "N/A";
    }
}
