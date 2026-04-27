package com.dhan.ticker.service;

import com.dhan.ticker.exception.WebSocketException;
import com.dhan.ticker.feed.TickPipeline;
import com.dhan.ticker.feed.TickStateService;
import com.dhan.ticker.feed.source.LiveWebSocketSource;
import com.dhan.ticker.model.ConnectRequest;
import com.dhan.ticker.model.IndexInstrument;
import com.dhan.ticker.model.TickData;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Backwards-compatible façade preserving the API consumed by
 * {@code WebSocketController} and {@code InstrumentSearchController}.
 *
 * <p>The previous monolithic implementation has been split into:
 * <ul>
 *   <li>{@link com.dhan.ticker.feed.decode.FrameDecoder} – pure byte-to-record decoder</li>
 *   <li>{@link TickStateService} – subscriptions, prev-day cache, enrich + broadcast</li>
 *   <li>{@link TickPipeline} – single source-agnostic frame entry point + pause gate</li>
 *   <li>{@link LiveWebSocketSource} – live Dhan WebSocket client + subscribe payload</li>
 * </ul>
 * A future replay reader plugs into {@link TickPipeline#accept} unchanged.
 */
@Service
public class MarketFeedService {

    private final LiveWebSocketSource liveSource;
    private final TickStateService stateService;
    private final TickPipeline pipeline;

    public MarketFeedService(LiveWebSocketSource liveSource,
                             TickStateService stateService,
                             TickPipeline pipeline) {
        this.liveSource = liveSource;
        this.stateService = stateService;
        this.pipeline = pipeline;
    }

    // ── Connection lifecycle (delegates to live source) ───────────────────

    public List<String> connect(List<ConnectRequest.InstrumentSub> instruments, String defaultFeedMode) {
        return liveSource.connect(instruments, defaultFeedMode);
    }

    public List<String> subscribe(List<ConnectRequest.InstrumentSub> instruments, String defaultFeedMode) {
        return liveSource.subscribe(instruments, defaultFeedMode);
    }

    public List<String> unsubscribe(List<String> securityIds) {
        return liveSource.unsubscribe(securityIds);
    }

    public void disconnect() {
        liveSource.disconnect();
    }

    public void pause() {
        if (!liveSource.isConnected()) throw new WebSocketException("WebSocket is not connected");
        pipeline.pause();
    }

    public void resume() {
        if (!liveSource.isConnected()) throw new WebSocketException("WebSocket is not connected");
        pipeline.resume();
    }

    public boolean isConnected() { return liveSource.isConnected(); }
    public boolean isPaused()    { return pipeline.isPaused(); }

    // ── Subscription group management (delegates to state service) ───────

    public void registerGroup(String indexSymbol, Set<String> securityIds) {
        stateService.registerGroup(indexSymbol, securityIds);
    }

    public Map<String, Object> unsubscribeBySymbol(String indexSymbol) {
        Map<String, Object> result = stateService.unsubscribeBySymbol(indexSymbol);
        Boolean noGroupsRemaining = (Boolean) result.remove("noGroupsRemaining");
        if (Boolean.TRUE.equals(noGroupsRemaining)) {
            disconnect();
            // refresh totalSubscribed after disconnect-clears
            result.put("totalSubscribed", 0);
        }
        return result;
    }

    public Map<String, Integer> getSubscriptionGroups() {
        return stateService.getSubscriptionGroups();
    }

    public List<IndexInstrument> getInstrumentsByGroup(String indexSymbol) {
        return stateService.getInstrumentsByGroup(indexSymbol);
    }

    // ── REST pre-seed ─────────────────────────────────────────────────────

    public void setPrevDayOi(Map<String, Long> prevOiMap) {
        stateService.setPrevDayOi(prevOiMap);
    }

    public void setInitialOiData(Map<String, Long> prevOiMap, Map<String, Long> currentOiMap) {
        stateService.setInitialOiData(prevOiMap, currentOiMap);
    }

    public void setInitialSpotOhlc(String securityId,
                                   double open, double high, double low, double close, double ltp) {
        stateService.setInitialSpotOhlc(securityId, open, high, low, close, ltp);
    }

    // ── Read accessors ────────────────────────────────────────────────────

    public List<IndexInstrument> getSubscribedInstruments() {
        return stateService.getSubscribedInstruments();
    }

    public Map<String, TickData> getLastTicks() {
        return stateService.getLastTicks();
    }

    public Map<String, Long> getPrevDayOiMap() {
        return stateService.getPrevDayOiMap();
    }
}
