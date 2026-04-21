package com.dhan.ticker.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class TickData {

    private String symbol;
    private String type;           // INDEX, EQUITY, FUTURE, OPTION
    private String securityId;
    private String exchangeSegment;

    // Price fields
    private double ltp;
    private double open;
    private double high;
    private double low;
    private double close;

    // Volume / quantity
    private long volume;
    private double atp;
    private long buyQty;
    private long sellQty;
    private int ltq;
    private long oi;
    private long oiChange;          // current OI - previous day close OI
    private long prevDayOi;         // previous day's closing OI (from PrevClose packet)

    // Option-specific
    private String strikePrice;
    private String optionType;     // CE, PE
    private String expiryDate;
    private String tradingSymbol;

    // Timestamp (ISO format)
    private String timestamp;

    // Market depth (5 levels for FULL mode)
    private List<DepthLevel> depth;

    @Data
    @Builder
    @NoArgsConstructor
    @AllArgsConstructor
    public static class DepthLevel {
        private int level;
        private double bidPrice;
        private int bidQty;
        private double askPrice;
        private int askQty;
    }
}
