package com.dhan.ticker.model.replay;

/**
 * Single line of {@code frames.idx.ndjson}. Maps directly to a slice of
 * {@code frames.bin}: read {@code length} bytes starting at {@code offset}.
 */
public record FrameIndexEntry(long timestampNanos, long offset, int length, int frameType) {}
