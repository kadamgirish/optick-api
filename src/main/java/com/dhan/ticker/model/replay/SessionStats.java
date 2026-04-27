package com.dhan.ticker.model.replay;

import lombok.Builder;
import lombok.Data;

import java.util.Map;

/**
 * Aggregate stats from a single pass over {@code frames.idx.ndjson}.
 */
@Data
@Builder
public class SessionStats {
    private String sessionId;
    private long frameCount;
    private long firstTimestampNanos;
    private long lastTimestampNanos;
    private long durationMillis;
    private long totalFrameBytes;
    private Map<Integer, Long> framesByType;
}
