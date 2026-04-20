package com.dhan.ticker.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class IndexInstrument {

    private String symbol;
    private String securityId;
    private String exchangeSegment;
    private String instrumentType;
    private String displayName;
    private String expiryDate;
    private String strikePrice;
    private String optionType;
    private String tradingSymbol;
}
