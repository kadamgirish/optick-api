package com.dhan.ticker.feed.decode;

import com.dhan.ticker.model.TickData;
import org.springframework.stereotype.Component;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Pure, stateless decoder for Dhan WebSocket binary frames.
 *
 * <p>Identical byte offsets / endianness to the previous inline implementation
 * in {@code MarketFeedService}. Knows nothing about subscriptions, market hours,
 * or STOMP — it only translates bytes to typed records.
 */
@Component
public class FrameDecoder {

    private static final Map<Integer, String> EXCHANGE_SEGMENT_NAMES = Map.of(
            0, "IDX_I",
            1, "NSE_EQ",
            2, "NSE_FNO",
            3, "NSE_CURRENCY",
            4, "BSE_EQ",
            5, "MCX_COMM",
            7, "BSE_CURRENCY",
            8, "BSE_FNO"
    );

    /**
     * Decode a single Dhan binary frame.
     *
     * @return a typed {@link DecodedPacket}, or {@code null} when the buffer is
     *         too short to even contain the 8-byte common header.
     */
    public DecodedPacket decode(ByteBuffer buffer) {
        if (buffer == null) return null;
        buffer.order(ByteOrder.LITTLE_ENDIAN);
        if (buffer.remaining() < 8) return null;

        int responseCode = Byte.toUnsignedInt(buffer.get(0));
        int exchangeSegmentCode = Byte.toUnsignedInt(buffer.get(3));
        int securityId = buffer.getInt(4);
        String segment = EXCHANGE_SEGMENT_NAMES.getOrDefault(exchangeSegmentCode,
                "UNKNOWN(" + exchangeSegmentCode + ")");

        return switch (responseCode) {
            case 1, 2 -> decodeTicker(buffer, responseCode, segment, securityId);
            case 4    -> decodeQuote(buffer, responseCode, segment, securityId);
            case 5    -> decodeOi(buffer, responseCode, segment, securityId);
            case 6    -> decodePrevClose(buffer, responseCode, segment, securityId);
            case 7    -> new DecodedPacket.MarketStatus(responseCode, segment, securityId);
            case 8    -> decodeFull(buffer, responseCode, segment, securityId);
            case 50   -> decodeDisconnect(buffer, responseCode, segment, securityId);
            default   -> new DecodedPacket.Unknown(responseCode, segment, securityId);
        };
    }

    private DecodedPacket decodeTicker(ByteBuffer buf, int rc, String seg, int secId) {
        if (buf.remaining() < 16) return null;
        float ltp = buf.getFloat(8);
        int ltt = buf.getInt(12);
        return new DecodedPacket.Ticker(rc, seg, secId, ltp, ltt);
    }

    private DecodedPacket decodeQuote(ByteBuffer buf, int rc, String seg, int secId) {
        if (buf.remaining() < 50) return null;
        float ltp   = buf.getFloat(8);
        short ltq   = buf.getShort(12);
        int   ltt   = buf.getInt(14);
        float atp   = buf.getFloat(18);
        int   vol   = buf.getInt(22);
        int   sellQ = buf.getInt(26);
        int   buyQ  = buf.getInt(30);
        float open  = buf.getFloat(34);
        float close = buf.getFloat(38);
        float high  = buf.getFloat(42);
        float low   = buf.getFloat(46);
        return new DecodedPacket.Quote(rc, seg, secId, ltp, ltq, ltt, atp, vol,
                sellQ, buyQ, open, close, high, low);
    }

    private DecodedPacket decodeFull(ByteBuffer buf, int rc, String seg, int secId) {
        if (buf.remaining() < 62) return null;
        float ltp   = buf.getFloat(8);
        short ltq   = buf.getShort(12);
        int   ltt   = buf.getInt(14);
        float atp   = buf.getFloat(18);
        int   vol   = buf.getInt(22);
        int   sellQ = buf.getInt(26);
        int   buyQ  = buf.getInt(30);
        int   oi    = buf.getInt(34);
        float open  = buf.getFloat(46);
        float close = buf.getFloat(50);
        float high  = buf.getFloat(54);
        float low   = buf.getFloat(58);

        List<TickData.DepthLevel> depth = null;
        if (buf.remaining() >= 162) {
            depth = new ArrayList<>(5);
            for (int i = 0; i < 5; i++) {
                int base = 62 + (i * 20);
                int bidQty  = buf.getInt(base);
                int askQty  = buf.getInt(base + 4);
                float bidPx = buf.getFloat(base + 12);
                float askPx = buf.getFloat(base + 16);
                depth.add(TickData.DepthLevel.builder()
                        .level(i + 1).bidPrice(bidPx).bidQty(bidQty)
                        .askPrice(askPx).askQty(askQty).build());
            }
        }
        return new DecodedPacket.Full(rc, seg, secId, ltp, ltq, ltt, atp, vol,
                sellQ, buyQ, oi, open, close, high, low, depth);
    }

    private DecodedPacket decodeOi(ByteBuffer buf, int rc, String seg, int secId) {
        if (buf.remaining() < 12) return null;
        int oi = buf.getInt(8);
        return new DecodedPacket.Oi(rc, seg, secId, oi);
    }

    private DecodedPacket decodePrevClose(ByteBuffer buf, int rc, String seg, int secId) {
        if (buf.remaining() < 16) return null;
        float prevClose = buf.getFloat(8);
        int prevOi = buf.getInt(12);
        return new DecodedPacket.PrevClose(rc, seg, secId, prevClose, prevOi);
    }

    private DecodedPacket decodeDisconnect(ByteBuffer buf, int rc, String seg, int secId) {
        if (buf.remaining() < 10) return null;
        short code = buf.getShort(8);
        return new DecodedPacket.Disconnect(rc, seg, secId, code);
    }
}
