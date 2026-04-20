package com.dhan.ticker.model;

import io.swagger.v3.oas.annotations.media.Schema;
import lombok.Data;

import java.util.List;

@Data
@Schema(description = "Request to connect WebSocket and subscribe to instruments")
public class ConnectRequest {

    @Schema(description = "List of instruments to subscribe",
            required = true)
    private List<InstrumentSub> instruments;

    @Schema(description = "Default feed mode if not specified per instrument: TICKER, QUOTE, FULL",
            example = "TICKER", allowableValues = {"TICKER", "QUOTE", "FULL"})
    private String feedMode = "TICKER";

    @Data
    @Schema(description = "Single instrument subscription")
    public static class InstrumentSub {

        @Schema(description = "Security ID (from /api/indices or Dhan master)", example = "13", required = true)
        private String securityId;

        @Schema(description = "Exchange segment (e.g. IDX_I, NSE_EQ, NSE_FNO)", example = "IDX_I")
        private String exchangeSegment;

        @Schema(description = "Feed mode override for this instrument (optional)",
                example = "TICKER", allowableValues = {"TICKER", "QUOTE", "FULL"})
        private String feedMode;
    }
}
