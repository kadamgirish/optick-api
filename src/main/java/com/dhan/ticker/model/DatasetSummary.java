package com.dhan.ticker.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class DatasetSummary {
    private String sessionId;
    private String basketPreset;
    private String feedMode;
    private String startTime;
    private String endTime;
    private long durationSeconds;
    private int instrumentCount;
    private long framesBinSizeBytes;
    private long indexFileSizeBytes;
}
