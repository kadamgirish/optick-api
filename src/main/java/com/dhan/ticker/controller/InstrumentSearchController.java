package com.dhan.ticker.controller;

import com.dhan.ticker.model.ApiResponse;
import com.dhan.ticker.model.ConnectRequest;
import com.dhan.ticker.model.IndexInstrument;
import com.dhan.ticker.service.MasterDataService;
import com.dhan.ticker.service.MasterDataService.ChainResult;
import com.dhan.ticker.service.MasterDataService.OiSnapshot;
import com.dhan.ticker.service.MarketFeedService;
import com.dhan.ticker.service.ReplayService;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.tags.Tag;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

@RestController
@RequestMapping("/api/instruments")
@Tag(name = "Instrument Search", description = "Search F&O instruments and get ready-to-use subscribe payloads")
public class InstrumentSearchController {

    private final MasterDataService masterDataService;
    private final MarketFeedService webSocketService;
    private final ReplayService replayService;

    public InstrumentSearchController(MasterDataService masterDataService,
                                      MarketFeedService webSocketService,
                                      ReplayService replayService) {
        this.masterDataService = masterDataService;
        this.webSocketService = webSocketService;
        this.replayService = replayService;
    }

    @GetMapping("/chain")
    @Operation(summary = "One-click: get spot + future + options chain with ready-to-paste payload",
            description = "Returns spot index (TICKER), nearest future (FULL), and nearest-expiry options " +
                    "(10 CE + 10 PE around ATM, FULL mode). Copy the connectPayload and POST to /api/ws/connect. " +
                    "Use spotPrice to set ATM reference, or leave blank for auto-detect. " +
                    "Use strikes param to change number of CE/PE (default 10 each).")
    public ResponseEntity<ChainResponse> chain(
            @RequestParam String symbol,
            @RequestParam(required = false) Double spotPrice,
            @RequestParam(defaultValue = "10") int strikes) {

        ChainResult chain = masterDataService.buildChain(symbol, spotPrice, strikes);

        // Build ready-to-paste connect payload
        List<ConnectRequest.InstrumentSub> subs = new ArrayList<>();

        // Spot index → TICKER (only mode IDX_I supports)
        if (chain.spot() != null) {
            ConnectRequest.InstrumentSub spot = new ConnectRequest.InstrumentSub();
            spot.setSecurityId(chain.spot().getSecurityId());
            spot.setExchangeSegment(chain.spot().getExchangeSegment());
            spot.setFeedMode("TICKER");
            subs.add(spot);
        }

        // Nearest future → FULL
        if (chain.nearestFuture() != null) {
            ConnectRequest.InstrumentSub fut = new ConnectRequest.InstrumentSub();
            fut.setSecurityId(chain.nearestFuture().getSecurityId());
            fut.setExchangeSegment(chain.nearestFuture().getExchangeSegment());
            subs.add(fut);
        }

        // CE options → FULL
        for (IndexInstrument ce : chain.callOptions()) {
            ConnectRequest.InstrumentSub sub = new ConnectRequest.InstrumentSub();
            sub.setSecurityId(ce.getSecurityId());
            sub.setExchangeSegment(ce.getExchangeSegment());
            subs.add(sub);
        }

        // PE options → FULL
        for (IndexInstrument pe : chain.putOptions()) {
            ConnectRequest.InstrumentSub sub = new ConnectRequest.InstrumentSub();
            sub.setSecurityId(pe.getSecurityId());
            sub.setExchangeSegment(pe.getExchangeSegment());
            subs.add(sub);
        }

        ConnectRequest payload = new ConnectRequest();
        payload.setFeedMode("FULL");
        payload.setInstruments(subs);

        return ResponseEntity.ok(new ChainResponse(
                symbol.toUpperCase(),
                chain.referencePrice(),
                chain.futureExpiry(),
                chain.optionExpiry(),
                chain.spot(),
                chain.nearestFuture(),
                chain.callOptions(),
                chain.putOptions(),
                subs.size(),
                payload
        ));
    }

    @PostMapping("/subscribe")
    @Operation(summary = "One-click: subscribe spot + future + 20 CE/PE options + underlying stocks",
            description = "Provide index symbol (e.g. NIFTY, BANKNIFTY). " +
                    "Automatically subscribes: spot (TICKER), nearest future (FULL), " +
                    "20 CE + 20 PE nearest expiry around ATM (FULL), and all underlying stocks (FULL). " +
                    "Check logs for live data.")
    public ResponseEntity<ApiResponse> quickSubscribe(@RequestParam String symbol) {

        // REPLAY MODE: use recorded securityIds from the loaded session's manifest.
        // Today's buildChain() would return a different expiry/ATM strikes than were recorded,
        // AND today's master CSV no longer contains expired contracts. So build IndexInstruments
        // directly from the manifest and bypass the master-CSV lookup entirely for F&O/INDEX.
        if (webSocketService.getFeedSource() == MarketFeedService.FeedSource.REPLAY) {
            List<IndexInstrument> direct = new ArrayList<>(
                    replayService.getIndexInstrumentsForIndex(symbol));

            // Add constituent stocks (equity secIds are stable across dates, master lookup works)
            List<IndexInstrument> stocks = masterDataService.getIndexConstituents(symbol);
            direct.addAll(stocks);

            if (direct.isEmpty()) {
                return ResponseEntity.badRequest()
                        .body(ApiResponse.error("No instruments found in recorded manifest for: " + symbol));
            }

            List<String> subscribed = webSocketService.prepareSubscriptionStateDirect(direct);
            Set<String> groupIds = direct.stream()
                    .map(ii -> ii.getExchangeSegment() + ":" + ii.getSecurityId())
                    .collect(Collectors.toSet());
            webSocketService.registerGroup(symbol, groupIds);

            long fnoCount = direct.size() - stocks.size();
            return ResponseEntity.ok(ApiResponse.ok(String.format(
                    "Subscribed %d instruments from replay manifest (F&O+index: %d, stocks: %d)",
                    subscribed.size(), fnoCount, stocks.size())));
        }

        // LIVE MODE (original path): build today's chain + current ATM + OI baseline
        // 1. Build chain: spot + nearest future + 20 CE/PE
        ChainResult chain = masterDataService.buildChain(symbol, null, 20);

        List<ConnectRequest.InstrumentSub> subs = new ArrayList<>();

        // Spot index → TICKER (only mode IDX_I supports)
        if (chain.spot() != null) {
            ConnectRequest.InstrumentSub s = new ConnectRequest.InstrumentSub();
            s.setSecurityId(chain.spot().getSecurityId());
            s.setExchangeSegment(chain.spot().getExchangeSegment());
            s.setFeedMode("TICKER");
            subs.add(s);
        }

        // Nearest future → FULL
        if (chain.nearestFuture() != null) {
            ConnectRequest.InstrumentSub f = new ConnectRequest.InstrumentSub();
            f.setSecurityId(chain.nearestFuture().getSecurityId());
            f.setExchangeSegment(chain.nearestFuture().getExchangeSegment());
            subs.add(f);
        }

        // 20 CE options → FULL
        for (IndexInstrument ce : chain.callOptions()) {
            ConnectRequest.InstrumentSub sub = new ConnectRequest.InstrumentSub();
            sub.setSecurityId(ce.getSecurityId());
            sub.setExchangeSegment(ce.getExchangeSegment());
            subs.add(sub);
        }

        // 20 PE options → FULL
        for (IndexInstrument pe : chain.putOptions()) {
            ConnectRequest.InstrumentSub sub = new ConnectRequest.InstrumentSub();
            sub.setSecurityId(pe.getSecurityId());
            sub.setExchangeSegment(pe.getExchangeSegment());
            subs.add(sub);
        }

        // Underlying constituent stocks → FULL
        List<IndexInstrument> stocks = masterDataService.getIndexConstituents(symbol);
        for (IndexInstrument stock : stocks) {
            ConnectRequest.InstrumentSub sub = new ConnectRequest.InstrumentSub();
            sub.setSecurityId(stock.getSecurityId());
            sub.setExchangeSegment(stock.getExchangeSegment());
            subs.add(sub);
        }

        if (subs.isEmpty()) {
            return ResponseEntity.badRequest()
                    .body(ApiResponse.error("No instruments found for symbol: " + symbol));
        }

        List<String> subscribed = webSocketService.connect(subs, "FULL");

        // Register this subscription group for smart unsubscribe
        Set<String> groupIds = subs.stream()
            .map(sub -> sub.getExchangeSegment() + ":" + sub.getSecurityId())
                .collect(Collectors.toSet());
        webSocketService.registerGroup(symbol, groupIds);

        // Fetch OI data: previous_oi + current_oi for options, current_oi for futures.
        Map<String, Long> prevOiMap = new java.util.HashMap<>();
        Map<String, Long> currOiMap = new java.util.HashMap<>();

        // Options: fetch both previous_oi and current_oi from Option Chain API
        if (chain.spot() != null && chain.optionExpiry() != null) {
            OiSnapshot snap = masterDataService.fetchOptionChainOi(
                    chain.spot().getSecurityId(),
                    chain.spot().getExchangeSegment(),
                    chain.optionExpiry());
            prevOiMap.putAll(snap.previousOi());
            currOiMap.putAll(snap.currentOi());
        }

        // Futures: fetch current OI from Market Quote API
        if (chain.nearestFuture() != null) {
            long futOi = masterDataService.fetchFutureOi(
                    chain.nearestFuture().getExchangeSegment(),
                    chain.nearestFuture().getSecurityId());
            if (futOi > 0) {
                currOiMap.put(chain.nearestFuture().getSecurityId(), futOi);
            }
        }

        // Pre-populate OI baseline + current OI for immediate oiChange computation
        if (!prevOiMap.isEmpty() || !currOiMap.isEmpty()) {
            webSocketService.setInitialOiData(prevOiMap, currOiMap);
        }

        int optCount = chain.callOptions().size() + chain.putOptions().size();
        String detail = String.format("Spot: %s (TICKER), Future: %s exp %s (FULL), " +
                        "Options: %d CE + %d PE exp %s (FULL), Stocks: %d (FULL)",
                chain.spot() != null ? chain.spot().getDisplayName() : "N/A",
                chain.nearestFuture() != null ? chain.nearestFuture().getTradingSymbol() : "N/A",
                chain.futureExpiry() != null ? chain.futureExpiry() : "N/A",
                chain.callOptions().size(), chain.putOptions().size(),
                chain.optionExpiry() != null ? chain.optionExpiry() : "N/A",
                stocks.size());

        return ResponseEntity.ok(ApiResponse.ok("Subscribed " + subscribed.size()
                + " instruments — " + detail));
    }
    @PostMapping("/unsubscribe")
    @Operation(summary = "Unsubscribe an index and all its instruments",
            description = "Removes spot, future, options, and constituent stocks for the given index. " +
                    "Stocks shared with other active subscriptions are kept. " +
                    "If no subscriptions remain, WebSocket disconnects automatically.")
    public ResponseEntity<ApiResponse> quickUnsubscribe(@RequestParam String symbol) {
        Map<String, Object> result = webSocketService.unsubscribeBySymbol(symbol);
        return ResponseEntity.ok(ApiResponse.ok(String.format(
                "Unsubscribed %s: %s removed, %s shared kept. Active: %s (total: %s)",
                result.get("symbol"), result.get("removed"), result.get("keptShared"),
                result.get("activeGroups"), result.get("totalSubscribed"))));
    }

    @GetMapping("/subscribed-groups")
    @Operation(summary = "List active subscription groups",
            description = "Returns which indices are currently subscribed and how many instruments each has.")
    public ResponseEntity<Map<String, Integer>> subscribedGroups() {
        return ResponseEntity.ok(webSocketService.getSubscriptionGroups());
    }

    @GetMapping("/subscribed-by-index/{symbol}")
    @Operation(summary = "Get all instruments subscribed for a specific index",
            description = "Returns the exact list of instruments (spot, future, options, constituent stocks) " +
                    "currently subscribed for the given index symbol. " +
                    "Returns empty array if the index is not currently subscribed. " +
                    "Use on page load or after subscribe to rebuild secId-to-index mappings on the frontend.")
    public ResponseEntity<List<IndexInstrument>> subscribedByIndex(@PathVariable String symbol) {
        return ResponseEntity.ok(webSocketService.getInstrumentsByGroup(symbol));
    }
    @GetMapping("/fno")
    @Operation(summary = "Search F&O instruments with filters",
            description = "Finds nearest expiry futures & options for a symbol. " +
                    "For options: use optionType=CE or PE, strike=24400 to filter.")
    public ResponseEntity<FnoSearchResponse> searchFnO(
            @RequestParam String symbol,
            @RequestParam(required = false) String type,
            @RequestParam(required = false) String optionType,
            @RequestParam(required = false) String strike,
            @RequestParam(defaultValue = "50") int limit) {

        List<IndexInstrument> instruments = masterDataService.searchFnO(
                symbol, type, optionType, strike, limit);

        String nearestExpiry = instruments.isEmpty() ? null : instruments.get(0).getExpiryDate();

        ConnectRequest payload = new ConnectRequest();
        payload.setFeedMode("FULL");
        List<ConnectRequest.InstrumentSub> subs = instruments.stream()
                .map(i -> {
                    ConnectRequest.InstrumentSub sub = new ConnectRequest.InstrumentSub();
                    sub.setSecurityId(i.getSecurityId());
                    sub.setExchangeSegment(i.getExchangeSegment());
                    return sub;
                })
                .toList();
        payload.setInstruments(subs);

        return ResponseEntity.ok(new FnoSearchResponse(
                symbol.toUpperCase(), nearestExpiry,
                instruments.size(), instruments, payload));
    }

    @GetMapping("/expiries")
    @Operation(summary = "List available expiry dates for a symbol",
            description = "Returns sorted list of future expiry dates >= today. " +
                    "Use type=FUTIDX or OPTIDX to filter.")
    public ResponseEntity<List<String>> getExpiries(
            @RequestParam String symbol,
            @RequestParam(required = false) String type) {
        return ResponseEntity.ok(masterDataService.getExpiries(symbol, type));
    }

    public record ChainResponse(
            String symbol,
            double referencePrice,
            String futureExpiry,
            String optionExpiry,
            IndexInstrument spot,
            IndexInstrument nearestFuture,
            List<IndexInstrument> callOptions,
            List<IndexInstrument> putOptions,
            int totalInstruments,
            ConnectRequest connectPayload
    ) {}

    public record FnoSearchResponse(
            String symbol,
            String nearestExpiry,
            int count,
            List<IndexInstrument> instruments,
            ConnectRequest connectPayload
    ) {}
}
