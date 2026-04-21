package com.dhan.ticker.controller;

import com.dhan.ticker.model.ApiResponse;
import com.dhan.ticker.model.DatasetSummary;
import com.dhan.ticker.service.ReplayService;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.tags.Tag;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.List;
import java.util.Map;

@RestController
@RequestMapping("/api/replay")
@Tag(name = "Replay", description = "Replay recorded WebSocket sessions from disk. " +
        "Flow: list → load → subscribe (normal POST /api/instruments/subscribe) → start → pause/resume/speed/seek → stop.")
public class ReplayController {

    private final ReplayService replayService;

    public ReplayController(ReplayService replayService) {
        this.replayService = replayService;
    }

    @GetMapping("/datasets")
    @Operation(summary = "List available recorded sessions",
            description = "Scans the configured data directory and returns one entry per session folder " +
                    "(manifest summary + file sizes). Use sessionId in POST /load.")
    public ResponseEntity<List<DatasetSummary>> datasets() {
        return ResponseEntity.ok(replayService.listDatasets());
    }

    @PostMapping("/load")
    @Operation(summary = "Load a recorded session into memory",
            description = "Parses the frame index (~4M entries per session) and opens frames.bin for random access. " +
                    "Activates REPLAY mode, clears any existing subscription state, and disables live WebSocket. " +
                    "After load, call POST /api/instruments/subscribe to build the subscription group " +
                    "(routes through prepareSubscriptionState — no live WS opened), then POST /api/replay/start.")
    public ResponseEntity<Map<String, Object>> load(@RequestParam String sessionId) {
        return ResponseEntity.ok(replayService.load(sessionId));
    }

    @PostMapping("/start")
    @Operation(summary = "Start playback from current cursor",
            description = "Launches the playback thread. Emits frames paced by their original inter-frame gap " +
                    "divided by the current speed multiplier.")
    public ResponseEntity<ApiResponse> start() {
        replayService.start();
        return ResponseEntity.ok(ApiResponse.ok("Playback started"));
    }

    @PostMapping("/pause")
    @Operation(summary = "Pause playback")
    public ResponseEntity<ApiResponse> pause() {
        replayService.pause();
        return ResponseEntity.ok(ApiResponse.ok("Paused"));
    }

    @PostMapping("/resume")
    @Operation(summary = "Resume playback")
    public ResponseEntity<ApiResponse> resume() {
        replayService.resume();
        return ResponseEntity.ok(ApiResponse.ok("Resumed"));
    }

    @PostMapping("/speed")
    @Operation(summary = "Set playback speed multiplier",
            description = "Any positive float. 1.0 = real-time; 10 = 10× faster; 0.5 = half-speed. " +
                    "Takes effect on the next inter-frame sleep.")
    public ResponseEntity<ApiResponse> speed(@RequestParam double value) {
        replayService.setSpeed(value);
        return ResponseEntity.ok(ApiResponse.ok("Speed set to " + value + "×"));
    }

    @PostMapping("/seek")
    @Operation(summary = "Seek to a wall-clock timestamp",
            description = "Pass ISO-8601 instant like 2026-04-17T05:30:00Z. Cursor jumps to the first frame " +
                    "at or after the given time (based on manifest startTime + frame nano offsets).")
    public ResponseEntity<ApiResponse> seek(@RequestParam String time) {
        replayService.seek(time);
        return ResponseEntity.ok(ApiResponse.ok("Seeked to " + time));
    }

    @PostMapping("/stop")
    @Operation(summary = "Stop playback and clear session",
            description = "Halts the playback thread, closes frames.bin, clears subscription state, " +
                    "and resets feed source to NONE.")
    public ResponseEntity<ApiResponse> stop() {
        replayService.stop();
        return ResponseEntity.ok(ApiResponse.ok("Stopped"));
    }

    @GetMapping("/status")
    @Operation(summary = "Current replay status",
            description = "Returns loaded/playing/paused flags, speed, cursor, totalFrames, progressPct, " +
                    "sessionStart, and currentTime (wall-clock of the current frame).")
    public ResponseEntity<Map<String, Object>> status() {
        return ResponseEntity.ok(replayService.status());
    }
}
