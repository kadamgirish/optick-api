package com.dhan.ticker.service;

import com.dhan.ticker.exception.InvalidInstrumentException;
import com.dhan.ticker.exception.WebSocketException;
import com.dhan.ticker.model.ConnectRequest;
import com.dhan.ticker.model.IndexInstrument;
import com.dhan.ticker.model.TickData;
import lombok.extern.slf4j.Slf4j;
import org.java_websocket.client.WebSocketClient;
import org.java_websocket.handshake.ServerHandshake;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.beans.BeanUtils;
import org.springframework.messaging.simp.SimpMessagingTemplate;
import org.springframework.stereotype.Service;

import java.net.URI;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.time.*;
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
public class MarketFeedService {

    @Value("${dhan.websocket-url}")
    private String websocketUrl;

    @Value("${dhan.access-token}")
    private String accessToken;

    @Value("${dhan.client-id}")
    private String clientId;

    private final MasterDataService masterDataService;
    private final SimpMessagingTemplate messagingTemplate;

    private final AtomicReference<WebSocketClient> wsClientRef = new AtomicReference<>();
    private final AtomicBoolean paused = new AtomicBoolean(false);
    private final AtomicBoolean connected = new AtomicBoolean(false);
    private final AtomicLong lastTickTime = new AtomicLong(0);
    private final Object subscriptionStateLock = new Object();

    // exchangeSegment:securityId -> instrument metadata for all subscribed instruments
    private final Map<String, IndexInstrument> subscribedInstruments = new ConcurrentHashMap<>();

    // indexSymbol -> set of exchangeSegment:securityId keys owned by that subscription group
    private final Map<String, Set<String>> subscriptionGroups = new ConcurrentHashMap<>();

    // securityId -> previous day's closing OI (from Option Chain API or PrevClose packet)
    private final Map<String, Long> prevDayOi = new ConcurrentHashMap<>();

    // securityId -> previous day's close price (from PrevClose packet)
    private final Map<String, Double> prevDayClose = new ConcurrentHashMap<>();

    /**
     * Pre-populate previous day OI map before ticks start flowing.
     * Called from controller after fetching from Dhan Option Chain API.
     */
    public void setPrevDayOi(Map<String, Long> prevOiMap) {
        prevDayOi.putAll(prevOiMap);
        log.info("Loaded previous day OI for {} instruments", prevOiMap.size());
    }

    /**
     * Pre-populate both previous OI and current OI on subscribe (crash recovery).
     * Sets prevDayOi baseline AND pre-populates lastTicks with oiChange so the dashboard
     * has OI change data immediately, even before WebSocket ticks arrive.
     */
    public void setInitialOiData(Map<String, Long> prevOiMap, Map<String, Long> currentOiMap) {
        synchronized (subscriptionStateLock) {
            prevDayOi.putAll(prevOiMap);
        }
        int preloaded = 0;
        for (Map.Entry<String, Long> entry : currentOiMap.entrySet()) {
            String secId = entry.getKey();
            long currOi = entry.getValue();
            Long prevOi;
            IndexInstrument inst;
            synchronized (subscriptionStateLock) {
                prevOi = prevDayOi.get(secId);
                inst = findSubscribedInstrumentBySecurityId(secId);
            }
            if (prevOi == null || currOi <= 0) continue;
            if (inst == null) continue;

            String symbol = inst.getSymbol();
            String type = detectType(inst);
            String tickKey = tickKey(symbol, inst);
            TickData tick = buildTickUpdate(tickKey, symbol, type, inst);
            tick.setOi((int) currOi);
            tick.setPrevDayOi(prevOi);
            tick.setOiChange(currOi - prevOi);
            tick.setTimestamp(TIME_FMT.format(java.time.ZonedDateTime.now(IST)));
            if (!storeTickIfSubscribed(inst, tickKey, tick)) {
                continue;
            }
            // Don't broadcast yet — tick has OI but no LTP. The WS full packet arriving
            // shortly will pull this cached tick via buildTickUpdate, merge LTP/OHLC,
            // and broadcast one complete frame (prevents a "LTP=0" flash in the UI).
            preloaded++;
        }
        log.info("[OI-PRELOAD] Cached {} instruments with OI/oiChange (prevDayOI={}, currentOI={})",
                preloaded, prevOiMap.size(), currentOiMap.size());
    }

    /**
     * Pre-populate OHLC for the spot index (IDX_I) from the REST OHLC API.
     * IDX_I websocket frames only carry LTP, so today's O/H/L and previous-day close
     * must be sourced from REST and merged into the cached tick.
     *
     * Dhan /v2/marketfeed/ohlc semantics:
     *   - last_price  = today's LTP
     *   - ohlc.open   = today's open
     *   - ohlc.high   = today's high
     *   - ohlc.low    = today's low
     *   - ohlc.close  = PREVIOUS trading day's close
     *
     * So we map REST `close` into `prevDayClose` (NOT tick.close, which our
     * domain uses for prev-day close via enrichTickWithPrevDayState).
     */
    public void setInitialSpotOhlc(String securityId,
                                   double open, double high, double low, double close, double ltp) {
        if (securityId == null) return;
        IndexInstrument inst;
        synchronized (subscriptionStateLock) {
            inst = findSubscribedInstrumentBySecurityId(securityId);
        }
        if (inst == null) {
            log.debug("[SPOT-OHLC] Skipped — no subscribed instrument for securityId={}", securityId);
            return;
        }

        if (close > 0) {
            prevDayClose.put(securityId, close);
        }

        String symbol = inst.getSymbol();
        String type = detectType(inst);
        String tickKey = tickKey(symbol, inst);
        TickData tick = buildTickUpdate(tickKey, symbol, type, inst);
        if (open > 0) tick.setOpen(open);
        if (high > 0) tick.setHigh(high);
        if (low > 0)  tick.setLow(low);
        // Seed LTP only if we don't already have one from WS
        if (ltp > 0 && tick.getLtp() == 0) tick.setLtp(ltp);
        tick.setTimestamp(TIME_FMT.format(java.time.ZonedDateTime.now(IST)));
        // Applies prevDayClose -> tick.close and oiChange
        enrichTickWithPrevDayState(inst, tick);

        if (!storeTickIfSubscribed(inst, tickKey, tick)) {
            return;
        }
        // Avoid broadcasting a tick with close=0 (user-visible "missing prevClose" on first frame).
        // When REST couldn't supply prev-close, the WS handlePrevClosePacket that follows within
        // ~150ms will enrich this stored tick and broadcast it with the correct close.
        boolean hasPrevClose = tick.getClose() > 0;
        if (hasPrevClose) {
            broadcast(tick, type);
        }
        log.info("[SPOT-OHLC] {} today: O={} H={} L={} LTP={} | prevDayClose(REST close)={} -> tick.close={} | broadcast={}",
                symbol, open, high, low, ltp, close, tick.getClose(), hasPrevClose);
    }

    private final ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();
    private ScheduledFuture<?> noDataWatcher;

    private static final ZoneId IST = ZoneId.of("Asia/Kolkata");
    private static final LocalTime MARKET_OPEN  = LocalTime.of(9, 0);
    private static final LocalTime MARKET_CLOSE = LocalTime.of(15, 30);
    private final AtomicBoolean marketClosedLogged = new AtomicBoolean(false);

    // symbol -> last received TickData (always updated, survives market close)
    private final Map<String, TickData> lastTicks = new ConcurrentHashMap<>();
    // symbols for which we already broadcast the closing tick
    private final Set<String> closingTickSent = ConcurrentHashMap.newKeySet();

    private static final DateTimeFormatter TIME_FMT = DateTimeFormatter
            .ofPattern("yyyy-MM-dd HH:mm:ss")
            .withZone(IST);

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

    public MarketFeedService(MasterDataService masterDataService,
                             SimpMessagingTemplate messagingTemplate) {
        this.masterDataService = masterDataService;
        this.messagingTemplate = messagingTemplate;
    }

    // ── Subscription group tracking ────────────────────────────────────────

    public void registerGroup(String indexSymbol, Set<String> securityIds) {
        subscriptionGroups.put(indexSymbol.toUpperCase(), securityIds);
        log.info("[GROUP] Registered '{}' with {} instruments", indexSymbol.toUpperCase(), securityIds.size());
    }

    public Map<String, Object> unsubscribeBySymbol(String indexSymbol) {
        String key = indexSymbol.toUpperCase();
        Set<String> groupIds = subscriptionGroups.remove(key);
        if (groupIds == null || groupIds.isEmpty()) {
            throw new WebSocketException("No subscription group found for: " + key);
        }

        // Compute IDs still needed by other groups
        Set<String> sharedIds = new HashSet<>();
        for (Set<String> otherGroup : subscriptionGroups.values()) {
            sharedIds.addAll(otherGroup);
        }

        // Remove only IDs not needed by any other group
        Set<String> toRemove = new HashSet<>(groupIds);
        toRemove.removeAll(sharedIds);
        int kept = groupIds.size() - toRemove.size();

        synchronized (subscriptionStateLock) {
            removeSubscriptionState(toRemove);
        }

        log.info("[UNSUB] Removed '{}': {} instruments removed, {} kept (shared with other indices)",
                key, toRemove.size(), kept);

        // If no groups remain, disconnect entirely
        if (subscriptionGroups.isEmpty()) {
            disconnect();
            log.info("[UNSUB] No active subscriptions remain — WebSocket disconnected");
        }

        Map<String, Object> result = new LinkedHashMap<>();
        result.put("symbol", key);
        result.put("removed", toRemove.size());
        result.put("keptShared", kept);
        result.put("activeGroups", new ArrayList<>(subscriptionGroups.keySet()));
        result.put("totalSubscribed", subscribedInstruments.size());
        return result;
    }

    public Map<String, Integer> getSubscriptionGroups() {
        Map<String, Integer> groups = new LinkedHashMap<>();
        for (Map.Entry<String, Set<String>> e : subscriptionGroups.entrySet()) {
            groups.put(e.getKey(), e.getValue().size());
        }
        return groups;
    }

    public List<IndexInstrument> getInstrumentsByGroup(String indexSymbol) {
        Set<String> keys = subscriptionGroups.get(indexSymbol.toUpperCase());
        if (keys == null || keys.isEmpty()) {
            return Collections.emptyList();
        }
        return keys.stream()
                .map(subscribedInstruments::get)
                .filter(Objects::nonNull)
                .collect(Collectors.toList());
    }

    // ── Connect & subscribe to multiple instruments ───────────────────────

    public List<String> connect(List<ConnectRequest.InstrumentSub> instruments, String defaultFeedMode) {
        if (connected.get()) {
            // Already connected — append new instruments instead of reconnecting
            return subscribe(instruments, defaultFeedMode);
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
                subscribedInstruments.put(instrumentStateKey(r.instrument), r.instrument);
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
        synchronized (subscriptionStateLock) {
            for (ResolvedSub r : resolved) {
                subscribedInstruments.put(instrumentStateKey(r.instrument), r.instrument);
                added.add(r.instrument.getSymbol() + "(" + r.instrument.getSecurityId() + ")");
            }
        }
        return added;
    }

    // ── Unsubscribe specific instruments ──────────────────────────────────

    public List<String> unsubscribe(List<String> securityIds) {
        if (!connected.get()) {
            throw new WebSocketException("WebSocket is not connected");
        }
        List<String> removed = new ArrayList<>();
        synchronized (subscriptionStateLock) {
            Set<String> stateKeysToRemove = new HashSet<>();
            for (String sid : securityIds) {
                for (String stateKey : matchingStateKeys(sid)) {
                    IndexInstrument inst = subscribedInstruments.get(stateKey);
                    if (inst != null) {
                        removed.add(inst.getSymbol() + "(" + sid + ")");
                        stateKeysToRemove.add(stateKey);
                    }
                }
            }
            removeSubscriptionState(stateKeysToRemove);
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
        synchronized (subscriptionStateLock) {
            subscribedInstruments.clear();
            subscriptionGroups.clear();
            prevDayOi.clear();
            lastTicks.clear();
            closingTickSent.clear();
        }
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

    public Map<String, Long> getPrevDayOiMap() {
        return new HashMap<>(prevDayOi);
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
            IndexInstrument inst = subscribedInstruments.get(stateKey(exchangeSegment, String.valueOf(securityId)));
            if (inst == null) return; // not subscribed — skip silently
            String symbol = inst.getSymbol();
            String type = detectType(inst);

            boolean marketOpen = isMarketOpen();

            switch (responseCode) {
                case 1, 2 -> handleTickerPacket(buffer, symbol, type, inst, marketOpen);
                case 4    -> handleQuotePacket(buffer, symbol, type, inst, marketOpen);
                case 5    -> handleOIPacket(buffer, symbol);
                case 6    -> handlePrevClosePacket(buffer, symbol);
                case 7    -> log.info("[MARKET_STATUS] Exchange: {}, SecurityId: {}", exchangeSegment, securityId);
                case 8    -> handleFullPacket(buffer, symbol, type, inst, marketOpen);
                case 50   -> handleDisconnectPacket(buffer);
                default   -> log.debug("Unknown response code {} securityId: {}", responseCode, securityId);
            }
        } catch (Exception e) {
            log.error("Error parsing binary message: {}", e.getMessage(), e);
        }
    }

    // ── Ticker Packet (code 1=Index, 2=Ticker) ───────────────────────────
    private void handleTickerPacket(ByteBuffer buf, String symbol, String type, IndexInstrument inst, boolean marketOpen) {
        if (buf.remaining() < 16) return;
        float ltp = buf.getFloat(8);
        int ltt = buf.getInt(12);

        String tKey = tickKey(symbol, inst);
        TickData tick = buildTickUpdate(tKey, symbol, type, inst);
        tick.setLtp(ltp);
        tick.setTimestamp(fmtTime(ltt));
        enrichTickWithPrevDayState(inst, tick);
        if (!storeTickIfSubscribed(inst, tKey, tick)) {
            return;
        }

        // For indices (TICKER-only, no prev-close in packet), defer the first
        // broadcast until prev-close is known — else UI sees close=0 and never
        // updates (closingTickSent is one-shot off-hours). Futures/options still
        // broadcast immediately since their packets carry richer state.
        boolean isIndex = "INDEX".equals(type);
        boolean waitingForPrevClose = isIndex && tick.getClose() <= 0;

        if (marketOpen) {
            closingTickSent.remove(tKey);
            log.info("[TICK] SYMBOL={}, TYPE={}, LTP={}, TIME={}",
                    symbol, type, fmt(ltp), fmtTime(ltt));
            if (!waitingForPrevClose) broadcast(tick, type);
        } else if (closingTickSent.add(tKey)) {
            log.info("[CLOSING-TICK] SYMBOL={}, TYPE={}, LTP={}, TIME={}, close={}",
                    symbol, type, fmt(ltp), fmtTime(ltt), tick.getClose());
            if (waitingForPrevClose) {
                // Allow a later ticker/prev-close path to do the first broadcast.
                closingTickSent.remove(tKey);
            } else {
                broadcast(tick, type);
            }
        }
    }

    // ── Quote Packet (code 4) ─────────────────────────────────────────────
    private void handleQuotePacket(ByteBuffer buf, String symbol, String type, IndexInstrument inst, boolean marketOpen) {
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

        String tKey = tickKey(symbol, inst);
        TickData tick = buildTickUpdate(tKey, symbol, type, inst);
        tick.setLtp(ltp);
        tick.setOpen(open);
        tick.setHigh(high);
        tick.setLow(low);
        tick.setClose(close);
        tick.setVolume(vol);
        tick.setAtp(atp);
        tick.setBuyQty(buyQ);
        tick.setSellQty(sellQ);
        tick.setLtq(ltq);
        tick.setTimestamp(fmtTime(ltt));
        enrichTickWithPrevDayState(inst, tick);

        if (!storeTickIfSubscribed(inst, tKey, tick)) {
            return;
        }

        if (marketOpen) {
            closingTickSent.remove(tKey);
            log.info("[TICK] SYMBOL={}, TYPE={}, LTP={}, OPEN={}, HIGH={}, LOW={}, CLOSE={}, " +
                            "VOL={}, ATP={}, BUY_QTY={}, SELL_QTY={}, LTQ={}, TIME={}",
                    symbol, type, fmt(ltp), fmt(open), fmt(high), fmt(low), fmt(close),
                    vol, fmt(atp), buyQ, sellQ, ltq, fmtTime(ltt));
            broadcast(tick, type);
        } else if (closingTickSent.add(tKey)) {
            log.info("[CLOSING-TICK] SYMBOL={}, TYPE={}, LTP={}, TIME={}",
                    symbol, type, fmt(ltp), fmtTime(ltt));
            broadcast(tick, type);
        }
    }

    // ── Full Packet (code 8) ──────────────────────────────────────────────
    private void handleFullPacket(ByteBuffer buf, String symbol, String type, IndexInstrument inst, boolean marketOpen) {
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

        String tKey = tickKey(symbol, inst);
        TickData tick = buildTickUpdate(tKey, symbol, type, inst);
        tick.setLtp(ltp);
        tick.setOpen(open);
        tick.setHigh(high);
        tick.setLow(low);
        tick.setClose(close);
        tick.setVolume(vol);
        tick.setAtp(atp);
        tick.setBuyQty(buyQ);
        tick.setSellQty(sellQ);
        tick.setLtq(ltq);
        tick.setOi(oi);
        if (oi > 0 && inst != null) {
            Long dayOpenOi = prevDayOi.get(inst.getSecurityId());
            if (dayOpenOi != null) {
                tick.setPrevDayOi(dayOpenOi);
                tick.setOiChange(oi - dayOpenOi);
                if (marketOpen) {
                    log.info("[OI-CHANGE] {} secId={} currentOI={} prevDayOI={} oiChange={}",
                            inst.getTradingSymbol(), inst.getSecurityId(), oi, dayOpenOi, oi - dayOpenOi);
                }
            } else {
                log.debug("[OI-CHANGE] {} secId={} currentOI={} — no prevDayOi baseline",
                        inst.getTradingSymbol(), inst.getSecurityId(), oi);
            }
        }
        tick.setTimestamp(fmtTime(ltt));
        enrichTickWithPrevDayState(inst, tick);

        if (buf.remaining() >= 162) {
            sb.append(" | DEPTH:");
            List<TickData.DepthLevel> depthLevels = new ArrayList<>();
            for (int i = 0; i < 5; i++) {
                int base = 62 + (i * 20);
                int bidQty  = buf.getInt(base);
                int askQty  = buf.getInt(base + 4);
                float bidPx = buf.getFloat(base + 12);
                float askPx = buf.getFloat(base + 16);
                sb.append(String.format(" [L%d B:%s(%d) A:%s(%d)]",
                        i + 1, fmt(bidPx), bidQty, fmt(askPx), askQty));
                depthLevels.add(TickData.DepthLevel.builder()
                        .level(i + 1).bidPrice(bidPx).bidQty(bidQty)
                        .askPrice(askPx).askQty(askQty).build());
            }
            tick.setDepth(depthLevels);
        }
        sb.append(String.format(", TIME=%s", fmtTime(ltt)));

        if (!storeTickIfSubscribed(inst, tKey, tick)) {
            return;
        }

        if (marketOpen) {
            closingTickSent.remove(tKey);
            log.info(sb.toString());
            broadcast(tick, type);
        } else if (closingTickSent.add(tKey)) {
            log.info("[CLOSING-TICK] SYMBOL={}, TYPE={}, LTP={}, OI={}, TIME={}",
                    symbol, type, fmt(ltp), oi, fmtTime(ltt));
            broadcast(tick, type);
        }
    }

    private void handleOIPacket(ByteBuffer buf, String symbol) {
        if (buf.remaining() < 12) return;
        int oi = buf.getInt(8);
        // For futures: use first OI snapshot as fallback baseline if not already set
        int securityId = buf.getInt(4);
        IndexInstrument inst;
        synchronized (subscriptionStateLock) {
            inst = findSubscribedInstrumentBySecurityId(String.valueOf(securityId));
        }
        if (inst != null && oi > 0 && !prevDayOi.containsKey(inst.getSecurityId())) {
            String type = detectType(inst);
            if ("FUTURE".equals(type)) {
                synchronized (subscriptionStateLock) {
                    prevDayOi.put(inst.getSecurityId(), (long) oi);
                }
                log.info("[OI-BASELINE] Using first OI snapshot {} as baseline for future {}",
                        oi, inst.getTradingSymbol());
            }
        }
        log.info("[OI] SYMBOL={}, OI={}", symbol, oi);
    }

    private void handlePrevClosePacket(ByteBuffer buf, String symbol) {
        if (buf.remaining() < 16) return;
        float prevClose = buf.getFloat(8);
        int prevOi = buf.getInt(12);
        // Store previous day's closing OI as baseline for OI change calculation
        int securityId = buf.getInt(4);
        IndexInstrument inst;
        TickData tickToBroadcast = null;
        synchronized (subscriptionStateLock) {
            inst = findSubscribedInstrumentBySecurityId(String.valueOf(securityId));
            if (inst != null) {
                if (prevClose > 0) {
                    prevDayClose.put(inst.getSecurityId(), (double) prevClose);
                }
                if (prevOi > 0) {
                    prevDayOi.put(inst.getSecurityId(), (long) prevOi);
                }

                TickData existingTick = lastTicks.get(tickKey(symbol, inst));
                if (existingTick != null) {
                    enrichTickWithPrevDayState(inst, existingTick);
                    tickToBroadcast = existingTick;
                }
            }
        }
        log.info("[PREV_CLOSE] SYMBOL={}, PREV_CLOSE={}, PREV_OI={}",
                symbol, fmt(prevClose), prevOi);
        if (tickToBroadcast != null) {
            log.info("[MERGED-BROADCAST] {} -> O={} H={} L={} C={} LTP={} OI={} prevOI={}",
                    tickToBroadcast.getSymbol(),
                    tickToBroadcast.getOpen(), tickToBroadcast.getHigh(),
                    tickToBroadcast.getLow(), tickToBroadcast.getClose(),
                    tickToBroadcast.getLtp(), tickToBroadcast.getOi(),
                    tickToBroadcast.getPrevDayOi());
            broadcast(tickToBroadcast, tickToBroadcast.getType());
        }
    }

    private void handleDisconnectPacket(ByteBuffer buf) {
        if (buf.remaining() < 10) return;
        short code = buf.getShort(8);
        log.error("[DISCONNECT] Server disconnected. Code: {}", code);
        connected.set(false);
        stopNoDataWatcher();
    }

    // ── STOMP broadcast helpers ─────────────────────────────────────────

    private TickData buildBaseTick(String symbol, String type, IndexInstrument inst) {
        TickData tick = new TickData();
        tick.setSymbol(symbol);
        tick.setType(type);
        if (inst != null) {
            tick.setSecurityId(inst.getSecurityId());
            tick.setExchangeSegment(inst.getExchangeSegment());
            tick.setStrikePrice(inst.getStrikePrice());
            tick.setOptionType(inst.getOptionType());
            tick.setExpiryDate(inst.getExpiryDate());
            tick.setTradingSymbol(inst.getTradingSymbol());
        }
        return tick;
    }

    private TickData buildTickUpdate(String tickKey, String symbol, String type, IndexInstrument inst) {
        synchronized (subscriptionStateLock) {
            TickData existingTick = lastTicks.get(tickKey);
            if (existingTick != null) {
                TickData copy = new TickData();
                BeanUtils.copyProperties(existingTick, copy);
                return copy;
            }
        }
        return buildBaseTick(symbol, type, inst);
    }

    private void enrichTickWithPrevDayState(IndexInstrument inst, TickData tick) {
        if (inst == null || tick == null) {
            return;
        }

        Double dayClose = prevDayClose.get(inst.getSecurityId());
        if (dayClose != null && dayClose > 0) {
            tick.setClose(dayClose);
        }

        Long dayOpenOi = prevDayOi.get(inst.getSecurityId());
        if (dayOpenOi != null) {
            tick.setPrevDayOi(dayOpenOi);
            if (tick.getOi() > 0) {
                tick.setOiChange(tick.getOi() - dayOpenOi);
            }
        }
    }

    private void broadcast(TickData tick, String type) {
        try {
            messagingTemplate.convertAndSend("/topic/ticks", tick);
            String subTopic = switch (type) {
                case "INDEX"  -> "/topic/ticks/index";
                case "FUTURE" -> "/topic/ticks/futures";
                case "OPTION" -> "/topic/ticks/options";
                case "EQUITY" -> "/topic/ticks/stocks";
                default       -> null;
            };
            if (subTopic != null) {
                messagingTemplate.convertAndSend(subTopic, tick);
            }
        } catch (Exception e) {
            log.debug("STOMP broadcast failed (no subscribers?): {}", e.getMessage());
        }
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

    private String tickKey(String symbol, IndexInstrument inst) {
        return (inst != null && inst.getTradingSymbol() != null) ? inst.getTradingSymbol() : symbol;
    }

    private String stateKey(String exchangeSegment, String securityId) {
        return exchangeSegment + ":" + securityId;
    }

    private String instrumentStateKey(IndexInstrument inst) {
        return stateKey(inst.getExchangeSegment(), inst.getSecurityId());
    }

    // Dhan WebSocket sends ltt as IST-based epoch (not UTC). Subtract IST offset to get real UTC.
    private static final int IST_OFFSET_SECONDS = 19800; // 5h30m

    private String fmtTime(int epoch) {
        return epoch > 0 ? TIME_FMT.format(Instant.ofEpochSecond(epoch - IST_OFFSET_SECONDS)) : "N/A";
    }

    public Map<String, TickData> getLastTicks() {
        synchronized (subscriptionStateLock) {
            return filteredLastTicksSnapshot();
        }
    }

    private boolean storeTickIfSubscribed(IndexInstrument inst, String tickKey, TickData tick) {
        synchronized (subscriptionStateLock) {
            if (!subscribedInstruments.containsKey(instrumentStateKey(inst))) {
                return false;
            }
            lastTicks.put(tickKey, tick);
            return true;
        }
    }

    private void removeSubscriptionState(Collection<String> stateKeys) {
        for (String stateKey : stateKeys) {
            subscribedInstruments.remove(stateKey);
            prevDayClose.remove(extractSecurityId(stateKey));
            prevDayOi.remove(extractSecurityId(stateKey));
        }
        pruneUnsubscribedTicks();
    }

    private void pruneUnsubscribedTicks() {
        lastTicks.entrySet().removeIf(e -> {
            String secId = e.getValue().getSecurityId();
            String exchangeSegment = e.getValue().getExchangeSegment();
            return secId != null && exchangeSegment != null && !subscribedInstruments.containsKey(stateKey(exchangeSegment, secId));
        });
        closingTickSent.removeIf(tKey -> !lastTicks.containsKey(tKey));
    }

    private Map<String, TickData> filteredLastTicksSnapshot() {
        Map<String, TickData> snapshot = new LinkedHashMap<>();
        for (Map.Entry<String, TickData> entry : lastTicks.entrySet()) {
            String secId = entry.getValue().getSecurityId();
            String exchangeSegment = entry.getValue().getExchangeSegment();
            if (secId == null || exchangeSegment == null || subscribedInstruments.containsKey(stateKey(exchangeSegment, secId))) {
                snapshot.put(entry.getKey(), entry.getValue());
            }
        }
        return snapshot;
    }

    private IndexInstrument findSubscribedInstrumentBySecurityId(String securityId) {
        for (IndexInstrument inst : subscribedInstruments.values()) {
            if (securityId.equals(inst.getSecurityId())) {
                return inst;
            }
        }
        return null;
    }

    private Set<String> matchingStateKeys(String securityId) {
        return subscribedInstruments.keySet().stream()
                .filter(key -> securityId.equals(extractSecurityId(key)))
                .collect(Collectors.toSet());
    }

    private String extractSecurityId(String stateKey) {
        int separator = stateKey.indexOf(':');
        return separator >= 0 ? stateKey.substring(separator + 1) : stateKey;
    }

    private boolean isMarketOpen() {
        ZonedDateTime now = ZonedDateTime.now(IST);
        DayOfWeek day = now.getDayOfWeek();
        if (day == DayOfWeek.SATURDAY || day == DayOfWeek.SUNDAY) {
            if (marketClosedLogged.compareAndSet(false, true)) {
                log.info("[MARKET] Weekend — suppressing tick logs & broadcasts");
            }
            return false;
        }
        LocalTime time = now.toLocalTime();
        if (time.isBefore(MARKET_OPEN) || time.isAfter(MARKET_CLOSE)) {
            if (marketClosedLogged.compareAndSet(false, true)) {
                log.info("[MARKET] Closed (outside 09:00–15:30 IST) — suppressing tick logs & broadcasts");
            }
            return false;
        }
        marketClosedLogged.set(false); // reset so next close is logged
        closingTickSent.clear();       // reset so closing ticks are sent again next day
        return true;
    }
}
