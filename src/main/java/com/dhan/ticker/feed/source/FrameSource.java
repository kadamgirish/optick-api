package com.dhan.ticker.feed.source;

/**
 * Abstraction over a stream of raw Dhan binary frames. Implementations push
 * each frame into {@link com.dhan.ticker.feed.TickPipeline#accept}. The live
 * WebSocket client and the file-replay reader are the two implementations.
 */
public interface FrameSource {

    /** True while the source is actively producing frames. */
    boolean isOpen();

    /**
     * Stop producing frames and release resources. Idempotent.
     */
    void stop();
}
