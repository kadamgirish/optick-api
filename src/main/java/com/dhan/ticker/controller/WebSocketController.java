package com.dhan.ticker.controller;

import com.dhan.ticker.model.ApiResponse;
import com.dhan.ticker.model.ConnectRequest;
import com.dhan.ticker.model.IndexInstrument;
import com.dhan.ticker.model.TickData;
import com.dhan.ticker.service.DhanWebSocketService;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.tags.Tag;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.List;
import java.util.Map;

@RestController
@RequestMapping("/api/ws")
@Tag(name = "WebSocket Control", description = "Connect, subscribe, disconnect, pause, resume tick feed")
public class WebSocketController {

    private final DhanWebSocketService webSocketService;

    public WebSocketController(DhanWebSocketService webSocketService) {
        this.webSocketService = webSocketService;
    }

    @PostMapping("/connect")
    @Operation(summary = "Connect & subscribe to multiple instruments",
            description = "Opens a WebSocket connection and subscribes to all instruments in the list. " +
                    "Supports INDEX (IDX_I, auto-forced to TICKER), EQUITY (NSE_EQ), F&O (NSE_FNO), etc. " +
                    "You can mix instrument types in a single call.")
    public ResponseEntity<ApiResponse> connect(@RequestBody ConnectRequest request) {
        List<String> subscribed = webSocketService.connect(request.getInstruments(), request.getFeedMode());
        return ResponseEntity.ok(ApiResponse.ok("Connected. Subscribed: " + subscribed));
    }

    @PostMapping("/subscribe")
    @Operation(summary = "Add more instruments to existing connection",
            description = "Subscribes additional instruments on the already-open WebSocket. " +
                    "No need to reconnect — just add more instruments.")
    public ResponseEntity<ApiResponse> subscribe(@RequestBody ConnectRequest request) {
        List<String> added = webSocketService.subscribe(request.getInstruments(), request.getFeedMode());
        return ResponseEntity.ok(ApiResponse.ok("Added subscriptions: " + added));
    }

    @PostMapping("/unsubscribe")
    @Operation(summary = "Unsubscribe specific instruments",
            description = "Removes instruments by securityId from tick logging")
    public ResponseEntity<ApiResponse> unsubscribe(@RequestBody List<String> securityIds) {
        List<String> removed = webSocketService.unsubscribe(securityIds);
        return ResponseEntity.ok(ApiResponse.ok("Unsubscribed: " + removed));
    }

    @PostMapping("/disconnect")
    @Operation(summary = "Disconnect from Dhan WebSocket")
    public ResponseEntity<ApiResponse> disconnect() {
        webSocketService.disconnect();
        return ResponseEntity.ok(ApiResponse.ok("Disconnected"));
    }

    @PostMapping("/pause")
    @Operation(summary = "Pause tick logging (connection stays open)")
    public ResponseEntity<ApiResponse> pause() {
        webSocketService.pause();
        return ResponseEntity.ok(ApiResponse.ok("Tick logging paused"));
    }

    @PostMapping("/resume")
    @Operation(summary = "Resume tick logging")
    public ResponseEntity<ApiResponse> resume() {
        webSocketService.resume();
        return ResponseEntity.ok(ApiResponse.ok("Tick logging resumed"));
    }

    @GetMapping("/status")
    @Operation(summary = "Connection status + subscribed instruments")
    public ResponseEntity<StatusResponse> status() {
        return ResponseEntity.ok(new StatusResponse(
                webSocketService.isConnected(),
                webSocketService.isPaused(),
                webSocketService.getSubscribedInstruments()
        ));
    }

    @GetMapping("/subscribed-instruments")
    @Operation(summary = "List all currently subscribed instruments",
            description = "Returns the full list of subscribed IndexInstrument objects. " +
                    "Use this to build the Angular dashboard layout before ticks arrive.")
    public ResponseEntity<List<IndexInstrument>> subscribedInstruments() {
        return ResponseEntity.ok(webSocketService.getSubscribedInstruments());
    }

    @GetMapping("/last-ticks")
    @Operation(summary = "Last known tick for every subscribed instrument",
            description = "Returns the most recent TickData per symbol. Available even after market close — " +
                    "use this to populate the dashboard with closing data.")
    public ResponseEntity<Map<String, TickData>> lastTicks() {
        return ResponseEntity.ok(webSocketService.getLastTicks());
    }

    @GetMapping("/prev-day-oi")
    @Operation(summary = "Debug: show loaded previous day OI map",
            description = "Returns securityId -> previousDayOI for all instruments where baseline is set.")
    public ResponseEntity<Map<String, Long>> prevDayOi() {
        return ResponseEntity.ok(webSocketService.getPrevDayOiMap());
    }

    public record StatusResponse(
            boolean connected,
            boolean paused,
            List<IndexInstrument> subscribedInstruments
    ) {}
}
