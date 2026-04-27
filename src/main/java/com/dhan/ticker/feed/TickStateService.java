package com.dhan.ticker.feed;

import com.dhan.ticker.exception.WebSocketException;
import com.dhan.ticker.feed.decode.DecodedPacket;
import com.dhan.ticker.model.IndexInstrument;
import com.dhan.ticker.model.TickData;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.BeanUtils;
import org.springframework.messaging.simp.SimpMessagingTemplate;
import org.springframework.stereotype.Service;

import java.time.DayOfWeek;
import java.time.Instant;
import java.time.LocalTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import java.util.stream.Collectors;

/**
 * Owns all live tick state: subscribed instruments, last-tick cache,
 * previous-day OI/close baselines, and broadcast fan-out to STOMP.
 *
 * <p>Source-agnostic: receives already-decoded {@link DecodedPacket}s from
 * either the live WebSocket feed or a file replay reader. All cross-packet
 * merging behaviour (PrevClose re-broadcast, prev-day OI baseline, REST
 * pre-seed merge, market-hours gating) lives here.
 *
 * <p>Extracted verbatim from the prior monolithic {@code MarketFeedService};
 * field names, lock granularity, and broadcast topics are unchanged.
 */
@Slf4j
@Service
public class TickStateService {

    private final SimpMessagingTemplate messagingTemplate;

    // exchangeSegment:securityId -> instrument metadata for all subscribed instruments
    private final Map<String, IndexInstrument> subscribedInstruments = new ConcurrentHashMap<>();
    // indexSymbol -> set of exchangeSegment:securityId keys owned by that subscription group
    private final Map<String, Set<String>> subscriptionGroups = new ConcurrentHashMap<>();
    // securityId -> previous day's closing OI
    private final Map<String, Long> prevDayOi = new ConcurrentHashMap<>();
    // securityId -> previous day's close price
    private final Map<String, Double> prevDayClose = new ConcurrentHashMap<>();
    // tickKey -> last broadcast TickData
    private final Map<String, TickData> lastTicks = new ConcurrentHashMap<>();
    // tickKeys for which the closing-tick has already been sent (off-hours)
    private final Set<String> closingTickSent = ConcurrentHashMap.newKeySet();

    private final Object subscriptionStateLock = new Object();
    private final AtomicBoolean marketClosedLogged = new AtomicBoolean(false);
    private final AtomicBoolean replayMode = new AtomicBoolean(false);

    /**
     * Optional listener invoked when a server-initiated disconnect packet
     * (response code 50) is received. Live source registers a hook here.
     */
    private volatile Consumer<DecodedPacket.Disconnect> disconnectListener;

    private static final ZoneId IST = ZoneId.of("Asia/Kolkata");
    private static final LocalTime MARKET_OPEN  = LocalTime.of(9, 0);
    private static final LocalTime MARKET_CLOSE = LocalTime.of(15, 30);
    private static final int IST_OFFSET_SECONDS = 19800;
    private static final DateTimeFormatter TIME_FMT = DateTimeFormatter
            .ofPattern("yyyy-MM-dd HH:mm:ss")
            .withZone(IST);

    public TickStateService(SimpMessagingTemplate messagingTemplate) {
        this.messagingTemplate = messagingTemplate;
    }

    // ── Disconnect callback wiring ─────────────────────────────────────────

    public void setDisconnectListener(Consumer<DecodedPacket.Disconnect> listener) {
        this.disconnectListener = listener;
    }

    // ── Replay mode ───────────────────────────────────────────────────────

    public void setReplayMode(boolean enabled) {
        replayMode.set(enabled);
    }

    public boolean isReplayMode() {
        return replayMode.get();
    }

    /**
     * Seed the subscribed-instruments map directly from a recorded session
     * manifest, bypassing live subscribe payloads. Used by the replay reader.
     */
    public void replaySeedSubscribedInstruments(Collection<IndexInstrument> instruments) {
        synchronized (subscriptionStateLock) {
            for (IndexInstrument inst : instruments) {
                if (inst == null || inst.getExchangeSegment() == null || inst.getSecurityId() == null) continue;
                subscribedInstruments.put(instrumentStateKey(inst), inst);
            }
        }
        log.info("[REPLAY-SEED] Seeded {} instruments", instruments.size());
    }

    public void replayClearSubscribedInstruments() {
        synchronized (subscriptionStateLock) {
            subscribedInstruments.clear();
            lastTicks.clear();
            closingTickSent.clear();
            prevDayClose.clear();
            prevDayOi.clear();
        }
    }

    // ── Subscription registration (called from live source) ───────────────

    public void registerSubscribed(IndexInstrument inst) {
        synchronized (subscriptionStateLock) {
            subscribedInstruments.put(instrumentStateKey(inst), inst);
        }
    }

    public void clearAllSubscriptions() {
        synchronized (subscriptionStateLock) {
            subscribedInstruments.clear();
            subscriptionGroups.clear();
            prevDayOi.clear();
            lastTicks.clear();
            closingTickSent.clear();
        }
    }

    public List<String> unsubscribe(List<String> securityIds) {
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
        return removed;
    }

    // ── Group tracking ────────────────────────────────────────────────────

    public void registerGroup(String indexSymbol, Set<String> securityIds) {
        subscriptionGroups.put(indexSymbol.toUpperCase(), securityIds);
        log.info("[GROUP] Registered '{}' with {} instruments", indexSymbol.toUpperCase(), securityIds.size());
    }

    /**
     * Unsubscribe an entire group. Returns metadata for the controller layer.
     * Caller is responsible for triggering a full disconnect when no groups
     * remain (this service does not own the WebSocket lifecycle).
     */
    public Map<String, Object> unsubscribeBySymbol(String indexSymbol) {
        String key = indexSymbol.toUpperCase();
        Set<String> groupIds = subscriptionGroups.remove(key);
        if (groupIds == null || groupIds.isEmpty()) {
            throw new WebSocketException("No subscription group found for: " + key);
        }
        Set<String> sharedIds = new HashSet<>();
        for (Set<String> otherGroup : subscriptionGroups.values()) {
            sharedIds.addAll(otherGroup);
        }
        Set<String> toRemove = new HashSet<>(groupIds);
        toRemove.removeAll(sharedIds);
        int kept = groupIds.size() - toRemove.size();

        synchronized (subscriptionStateLock) {
            removeSubscriptionState(toRemove);
        }

        log.info("[UNSUB] Removed '{}': {} instruments removed, {} kept (shared with other indices)",
                key, toRemove.size(), kept);

        Map<String, Object> result = new LinkedHashMap<>();
        result.put("symbol", key);
        result.put("removed", toRemove.size());
        result.put("keptShared", kept);
        result.put("activeGroups", new ArrayList<>(subscriptionGroups.keySet()));
        result.put("totalSubscribed", subscribedInstruments.size());
        result.put("noGroupsRemaining", subscriptionGroups.isEmpty());
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

    // ── REST pre-seed entry points (unchanged behaviour) ──────────────────

    public void setPrevDayOi(Map<String, Long> prevOiMap) {
        prevDayOi.putAll(prevOiMap);
        log.info("Loaded previous day OI for {} instruments", prevOiMap.size());
    }

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
            tick.setTimestamp(TIME_FMT.format(ZonedDateTime.now(IST)));
            if (!storeTickIfSubscribed(inst, tickKey, tick)) {
                continue;
            }
            preloaded++;
        }
        log.info("[OI-PRELOAD] Cached {} instruments with OI/oiChange (prevDayOI={}, currentOI={})",
                preloaded, prevOiMap.size(), currentOiMap.size());
    }

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
        if (ltp > 0 && tick.getLtp() == 0) tick.setLtp(ltp);
        tick.setTimestamp(TIME_FMT.format(ZonedDateTime.now(IST)));
        enrichTickWithPrevDayState(inst, tick);

        if (!storeTickIfSubscribed(inst, tickKey, tick)) {
            return;
        }
        boolean hasPrevClose = tick.getClose() > 0;
        if (hasPrevClose) {
            broadcast(tick, type);
        }
        log.info("[SPOT-OHLC] {} today: O={} H={} L={} LTP={} | prevDayClose(REST close)={} -> tick.close={} | broadcast={}",
                symbol, open, high, low, ltp, close, tick.getClose(), hasPrevClose);
    }

    // ── Read accessors (controllers) ──────────────────────────────────────

    public List<IndexInstrument> getSubscribedInstruments() {
        return new ArrayList<>(subscribedInstruments.values());
    }

    public Map<String, Long> getPrevDayOiMap() {
        return new HashMap<>(prevDayOi);
    }

    public Map<String, TickData> getLastTicks() {
        synchronized (subscriptionStateLock) {
            return filteredLastTicksSnapshot();
        }
    }

    // ── Single packet entry point ─────────────────────────────────────────

    public void onPacket(DecodedPacket pkt) {
        if (pkt == null) return;
        try {
            if (pkt instanceof DecodedPacket.Disconnect d) {
                handleDisconnect(d);
                return;
            }
            if (pkt instanceof DecodedPacket.MarketStatus) {
                log.info("[MARKET_STATUS] Exchange: {}, SecurityId: {}",
                        pkt.exchangeSegment(), pkt.securityId());
                return;
            }
            if (pkt instanceof DecodedPacket.Unknown) {
                log.debug("Unknown response code {} securityId: {}",
                        pkt.responseCode(), pkt.securityId());
                return;
            }

            IndexInstrument inst = subscribedInstruments.get(
                    stateKey(pkt.exchangeSegment(), String.valueOf(pkt.securityId())));
            if (inst == null) return; // not subscribed — silently skip

            String symbol = inst.getSymbol();
            String type = detectType(inst);
            boolean marketOpen = isMarketOpen();

            if (pkt instanceof DecodedPacket.Ticker t) {
                handleTicker(t, symbol, type, inst, marketOpen);
            } else if (pkt instanceof DecodedPacket.Quote q) {
                handleQuote(q, symbol, type, inst, marketOpen);
            } else if (pkt instanceof DecodedPacket.Full f) {
                handleFull(f, symbol, type, inst, marketOpen);
            } else if (pkt instanceof DecodedPacket.Oi o) {
                handleOi(o, symbol);
            } else if (pkt instanceof DecodedPacket.PrevClose pc) {
                handlePrevClose(pc, symbol);
            }
        } catch (Exception e) {
            log.error("Error processing packet {}: {}", pkt, e.getMessage(), e);
        }
    }

    // ── Per-packet handlers (logic lifted from prior MarketFeedService) ──

    private void handleTicker(DecodedPacket.Ticker pkt, String symbol, String type,
                              IndexInstrument inst, boolean marketOpen) {
        float ltp = pkt.ltp();
        int ltt = pkt.ltt();

        String tKey = tickKey(symbol, inst);
        TickData tick = buildTickUpdate(tKey, symbol, type, inst);
        tick.setLtp(ltp);
        tick.setTimestamp(fmtTime(ltt));
        enrichTickWithPrevDayState(inst, tick);
        if (!storeTickIfSubscribed(inst, tKey, tick)) {
            return;
        }

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
                closingTickSent.remove(tKey);
            } else {
                broadcast(tick, type);
            }
        }
    }

    private void handleQuote(DecodedPacket.Quote pkt, String symbol, String type,
                             IndexInstrument inst, boolean marketOpen) {
        String tKey = tickKey(symbol, inst);
        TickData tick = buildTickUpdate(tKey, symbol, type, inst);
        tick.setLtp(pkt.ltp());
        tick.setOpen(pkt.open());
        tick.setHigh(pkt.high());
        tick.setLow(pkt.low());
        tick.setClose(pkt.close());
        tick.setVolume(pkt.volume());
        tick.setAtp(pkt.atp());
        tick.setBuyQty(pkt.buyQty());
        tick.setSellQty(pkt.sellQty());
        tick.setLtq(pkt.ltq());
        tick.setTimestamp(fmtTime(pkt.ltt()));
        enrichTickWithPrevDayState(inst, tick);

        if (!storeTickIfSubscribed(inst, tKey, tick)) {
            return;
        }

        if (marketOpen) {
            closingTickSent.remove(tKey);
            log.info("[TICK] SYMBOL={}, TYPE={}, LTP={}, OPEN={}, HIGH={}, LOW={}, CLOSE={}, " +
                            "VOL={}, ATP={}, BUY_QTY={}, SELL_QTY={}, LTQ={}, TIME={}",
                    symbol, type, fmt(pkt.ltp()), fmt(pkt.open()), fmt(pkt.high()), fmt(pkt.low()),
                    fmt(pkt.close()), pkt.volume(), fmt(pkt.atp()),
                    pkt.buyQty(), pkt.sellQty(), pkt.ltq(), fmtTime(pkt.ltt()));
            broadcast(tick, type);
        } else if (closingTickSent.add(tKey)) {
            log.info("[CLOSING-TICK] SYMBOL={}, TYPE={}, LTP={}, TIME={}",
                    symbol, type, fmt(pkt.ltp()), fmtTime(pkt.ltt()));
            broadcast(tick, type);
        }
    }

    private void handleFull(DecodedPacket.Full pkt, String symbol, String type,
                            IndexInstrument inst, boolean marketOpen) {
        float ltp = pkt.ltp();
        short ltq = pkt.ltq();
        int ltt = pkt.ltt();
        float atp = pkt.atp();
        int vol = pkt.volume();
        int sellQ = pkt.sellQty();
        int buyQ = pkt.buyQty();
        int oi = pkt.oi();
        float open = pkt.open();
        float close = pkt.close();
        float high = pkt.high();
        float low = pkt.low();

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

        if (pkt.depth() != null) {
            sb.append(" | DEPTH:");
            for (TickData.DepthLevel d : pkt.depth()) {
                sb.append(String.format(" [L%d B:%s(%d) A:%s(%d)]",
                        d.getLevel(), fmt((float) d.getBidPrice()), d.getBidQty(),
                        fmt((float) d.getAskPrice()), d.getAskQty()));
            }
            tick.setDepth(pkt.depth());
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

    private void handleOi(DecodedPacket.Oi pkt, String symbol) {
        int oi = pkt.oi();
        IndexInstrument inst;
        synchronized (subscriptionStateLock) {
            inst = findSubscribedInstrumentBySecurityId(String.valueOf(pkt.securityId()));
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

    private void handlePrevClose(DecodedPacket.PrevClose pkt, String symbol) {
        float prevClose = pkt.prevClose();
        int prevOi = pkt.prevOi();
        IndexInstrument inst;
        TickData tickToBroadcast = null;
        synchronized (subscriptionStateLock) {
            inst = findSubscribedInstrumentBySecurityId(String.valueOf(pkt.securityId()));
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

    private void handleDisconnect(DecodedPacket.Disconnect pkt) {
        log.error("[DISCONNECT] Server disconnected. Code: {}", pkt.code());
        Consumer<DecodedPacket.Disconnect> listener = disconnectListener;
        if (listener != null) {
            try {
                listener.accept(pkt);
            } catch (Exception e) {
                log.warn("Disconnect listener threw: {}", e.getMessage());
            }
        }
    }

    // ── Tick build / enrich / store / broadcast helpers ───────────────────

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
            return secId != null && exchangeSegment != null
                    && !subscribedInstruments.containsKey(stateKey(exchangeSegment, secId));
        });
        closingTickSent.removeIf(tKey -> !lastTicks.containsKey(tKey));
    }

    private Map<String, TickData> filteredLastTicksSnapshot() {
        Map<String, TickData> snapshot = new LinkedHashMap<>();
        for (Map.Entry<String, TickData> entry : lastTicks.entrySet()) {
            String secId = entry.getValue().getSecurityId();
            String exchangeSegment = entry.getValue().getExchangeSegment();
            if (secId == null || exchangeSegment == null
                    || subscribedInstruments.containsKey(stateKey(exchangeSegment, secId))) {
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

    private String tickKey(String symbol, IndexInstrument inst) {
        return (inst != null && inst.getTradingSymbol() != null) ? inst.getTradingSymbol() : symbol;
    }

    private String stateKey(String exchangeSegment, String securityId) {
        return exchangeSegment + ":" + securityId;
    }

    private String instrumentStateKey(IndexInstrument inst) {
        return stateKey(inst.getExchangeSegment(), inst.getSecurityId());
    }

    private String fmt(float v) { return String.format("%.2f", v); }

    private String fmtTime(int epoch) {
        return epoch > 0 ? TIME_FMT.format(Instant.ofEpochSecond(epoch - IST_OFFSET_SECONDS)) : "N/A";
    }

    /** Replay treats market as always-open so all packets broadcast. */
    private boolean isMarketOpen() {
        if (replayMode.get()) return true;
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
        marketClosedLogged.set(false);
        closingTickSent.clear();
        return true;
    }

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
}
