package com.dhan.ticker.model.replay;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import lombok.Data;

import java.util.List;

/**
 * Mirrors {@code manifest.json} written by the recorder for each session.
 * Only fields we care about are mapped; unknown fields are ignored so future
 * recorder additions don't break parsing.
 */
@Data
@JsonIgnoreProperties(ignoreUnknown = true)
public class SessionManifest {

    private String sessionId;
    private String startTime;
    private String endTime;
    private String basketPreset;
    private String wsEndpoint;
    private String feedMode;
    private String recorderVersion;
    private List<String> subscribePayloads;
    private List<InstrumentEntry> instruments;

    @Data
    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class InstrumentEntry {
        private String tag;
        private long securityId;
        private String exchangeSegment;
        private String tradingSymbol;
        private String displaySymbol;
        private String expiry;
        private Double strike;
        private String optionType;
        private String instrumentType;
    }
}
