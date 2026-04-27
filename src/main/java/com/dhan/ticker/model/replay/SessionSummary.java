package com.dhan.ticker.model.replay;

import lombok.Builder;
import lombok.Data;

/**
 * Lightweight summary returned by {@code GET /api/replay/sessions} — built
 * from the manifest plus on-disk file sizes, no frame walking required.
 */
@Data
@Builder
public class SessionSummary {
    private String sessionId;
    private String startTime;
    private String endTime;
    private String basketPreset;
    private String feedMode;
    private int instrumentCount;
    private long framesBinBytes;
    private long framesIdxBytes;
}
