package com.dhan.ticker.controller;

import com.dhan.ticker.feed.source.ReplayFileSource;
import com.dhan.ticker.model.ApiResponse;
import com.dhan.ticker.model.replay.SessionManifest;
import com.dhan.ticker.model.replay.SessionStats;
import com.dhan.ticker.model.replay.SessionSummary;
import com.dhan.ticker.replay.FrameIndexStream;
import com.dhan.ticker.replay.SessionDiscoveryService;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.tags.Tag;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * Read-only session metadata + replay playback control.
 * Replay broadcasts to the same STOMP topics as live ({@code /topic/ticks/*})
 * so consumers don't need to distinguish between sources.
 */
@RestController
@RequestMapping("/api/replay")
@Tag(name = "Replay", description = "Read recorded WebSocket sessions from disk and replay them through the live tick pipeline")
public class ReplayController {

    private final SessionDiscoveryService discovery;
    private final FrameIndexStream indexStream;
    private final ReplayFileSource source;

    public ReplayController(SessionDiscoveryService discovery,
                            FrameIndexStream indexStream,
                            ReplayFileSource source) {
        this.discovery = discovery;
        this.indexStream = indexStream;
        this.source = source;
    }

    // ── Discovery / metadata ─────────────────────────────────────────────

    @GetMapping("/sessions")
    @Operation(summary = "List all recorded sessions on disk")
    public ResponseEntity<List<SessionSummary>> listSessions() {
        return ResponseEntity.ok(discovery.listSessions());
    }

    @GetMapping("/sessions/{sessionId}/manifest")
    @Operation(summary = "Full manifest for a single session")
    public ResponseEntity<SessionManifest> manifest(@PathVariable String sessionId) {
        return ResponseEntity.ok(discovery.getManifest(sessionId));
    }

    @GetMapping("/sessions/{sessionId}/instruments")
    @Operation(summary = "Instruments contained in a session manifest",
            description = "Optional filters: ?segment=NSE_FNO and/or ?instrumentType=OPTIDX")
    public ResponseEntity<List<SessionManifest.InstrumentEntry>> instruments(
            @PathVariable String sessionId,
            @RequestParam(required = false) String segment,
            @RequestParam(required = false) String instrumentType) {
        SessionManifest m = discovery.getManifest(sessionId);
        List<SessionManifest.InstrumentEntry> all = m.getInstruments() == null ? List.of() : m.getInstruments();
        return ResponseEntity.ok(all.stream()
                .filter(e -> segment == null || segment.equalsIgnoreCase(e.getExchangeSegment()))
                .filter(e -> instrumentType == null || instrumentType.equalsIgnoreCase(e.getInstrumentType()))
                .toList());
    }

    @GetMapping("/sessions/{sessionId}/stats")
    @Operation(summary = "Frame statistics from a single pass over frames.idx.ndjson")
    public ResponseEntity<SessionStats> stats(@PathVariable String sessionId) throws IOException {
        return ResponseEntity.ok(indexStream.computeStats(sessionId, discovery.resolveSessionDir(sessionId)));
    }

    // ── Playback control ─────────────────────────────────────────────────

    @PostMapping("/start")
    @Operation(summary = "Start replaying a session through the live tick pipeline",
            description = "Pushes recorded raw frames into the same decode/enrich/broadcast path as live data. " +
                    "Single active session at a time — call /stop first to switch.")
    public ResponseEntity<ApiResponse> start(
            @RequestParam String session,
            @RequestParam(required = false, defaultValue = "1.0") double speed) {
        source.start(session, speed);
        return ResponseEntity.ok(ApiResponse.ok("Replay started: " + session + " @ " + speed + "x"));
    }

    @PostMapping("/pause")
    @Operation(summary = "Pause replay (resume keeps relative timing)")
    public ResponseEntity<ApiResponse> pause() {
        source.pause();
        return ResponseEntity.ok(ApiResponse.ok("Replay paused"));
    }

    @PostMapping("/resume")
    @Operation(summary = "Resume a paused replay")
    public ResponseEntity<ApiResponse> resume() {
        source.resume();
        return ResponseEntity.ok(ApiResponse.ok("Replay resumed"));
    }

    @PostMapping("/stop")
    @Operation(summary = "Stop replay and clear seeded subscriptions")
    public ResponseEntity<ApiResponse> stop() {
        source.stop();
        return ResponseEntity.ok(ApiResponse.ok("Replay stopped"));
    }

    @GetMapping("/status")
    @Operation(summary = "Current replay state")
    public ResponseEntity<Map<String, Object>> status() {
        return ResponseEntity.ok(Map.of(
                "state", source.getState().name(),
                "sessionId", source.getCurrentSessionId() == null ? "" : source.getCurrentSessionId(),
                "framesPushed", source.getFramesPushed(),
                "currentTimestampNanos", source.getCurrentTimestampNanos()
        ));
    }
}
