package com.dhan.ticker.feed;

import com.dhan.ticker.feed.decode.DecodedPacket;
import com.dhan.ticker.feed.decode.FrameDecoder;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Single entry point for raw frame bytes from any source (live WebSocket or
 * file replay). Decodes via {@link FrameDecoder} and dispatches to
 * {@link TickStateService}. Owns the downstream pause gate that applies to
 * every source uniformly.
 */
@Slf4j
@Component
public class TickPipeline {

    private final FrameDecoder decoder;
    private final TickStateService state;

    private final AtomicBoolean paused = new AtomicBoolean(false);
    private final AtomicLong framesReceived = new AtomicLong(0);
    private final AtomicLong framesProcessed = new AtomicLong(0);

    public TickPipeline(FrameDecoder decoder, TickStateService state) {
        this.decoder = decoder;
        this.state = state;
    }

    /** Single entry point. Safe to call from any thread. */
    public void accept(ByteBuffer buffer) {
        framesReceived.incrementAndGet();
        if (paused.get()) return;
        DecodedPacket pkt = decoder.decode(buffer);
        if (pkt == null) return;
        state.onPacket(pkt);
        framesProcessed.incrementAndGet();
    }

    public void pause()  { paused.set(true); }
    public void resume() { paused.set(false); }
    public boolean isPaused() { return paused.get(); }

    public long getFramesReceived()  { return framesReceived.get(); }
    public long getFramesProcessed() { return framesProcessed.get(); }
}
