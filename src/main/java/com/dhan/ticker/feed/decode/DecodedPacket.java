package com.dhan.ticker.feed.decode;

import com.dhan.ticker.model.TickData;

import java.util.List;

/**
 * Source-agnostic representation of a single decoded Dhan binary frame.
 * Produced by {@link FrameDecoder} from either a live WebSocket payload or a
 * raw replay frame read from disk. Carries only the values pulled from bytes —
 * no instrument lookup, no enrichment, no state.
 */
public sealed interface DecodedPacket
        permits DecodedPacket.Ticker,
                DecodedPacket.Quote,
                DecodedPacket.Full,
                DecodedPacket.Oi,
                DecodedPacket.PrevClose,
                DecodedPacket.MarketStatus,
                DecodedPacket.Disconnect,
                DecodedPacket.Unknown {

    int responseCode();
    String exchangeSegment();
    int securityId();

    /** Response codes 1 (index ticker) and 2 (instrument ticker). */
    record Ticker(int responseCode, String exchangeSegment, int securityId,
                  float ltp, int ltt) implements DecodedPacket {}

    /** Response code 4. */
    record Quote(int responseCode, String exchangeSegment, int securityId,
                 float ltp, short ltq, int ltt, float atp, int volume,
                 int sellQty, int buyQty,
                 float open, float close, float high, float low) implements DecodedPacket {}

    /** Response code 8 — full packet (Quote fields + OI + optional 5-level depth). */
    record Full(int responseCode, String exchangeSegment, int securityId,
                float ltp, short ltq, int ltt, float atp, int volume,
                int sellQty, int buyQty, int oi,
                float open, float close, float high, float low,
                List<TickData.DepthLevel> depth) implements DecodedPacket {}

    /** Response code 5 — standalone OI snapshot. */
    record Oi(int responseCode, String exchangeSegment, int securityId,
              int oi) implements DecodedPacket {}

    /** Response code 6 — previous-day close + previous-day OI. */
    record PrevClose(int responseCode, String exchangeSegment, int securityId,
                     float prevClose, int prevOi) implements DecodedPacket {}

    /** Response code 7 — market status. */
    record MarketStatus(int responseCode, String exchangeSegment, int securityId)
            implements DecodedPacket {}

    /** Response code 50 — server-initiated disconnect. */
    record Disconnect(int responseCode, String exchangeSegment, int securityId,
                      short code) implements DecodedPacket {}

    /** Any unrecognised response code; carrier for diagnostics. */
    record Unknown(int responseCode, String exchangeSegment, int securityId)
            implements DecodedPacket {}
}
