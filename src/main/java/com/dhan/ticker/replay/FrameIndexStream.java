package com.dhan.ticker.replay;

import com.dhan.ticker.model.replay.FrameIndexEntry;
import com.dhan.ticker.model.replay.SessionStats;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

/**
 * Streams {@code frames.idx.ndjson} as {@link FrameIndexEntry} records.
 * Files can be 50MB+; never slurped into memory.
 */
@Slf4j
@Component
public class FrameIndexStream {

    private final ObjectMapper mapper = new ObjectMapper();

    /**
     * Open a streaming view of a session's frame index. Caller MUST close the
     * returned stream.
     */
    public Stream<FrameIndexEntry> stream(Path sessionDir) throws IOException {
        Path idx = sessionDir.resolve("frames.idx.ndjson");
        var lines = Files.lines(idx, StandardCharsets.UTF_8);
        return lines.map(this::parseLine).filter(e -> e != null);
    }

    /** Single pass aggregate. */
    public SessionStats computeStats(String sessionId, Path sessionDir) throws IOException {
        long count = 0;
        long firstTs = Long.MAX_VALUE;
        long lastTs = Long.MIN_VALUE;
        long totalBytes = 0;
        Map<Integer, Long> byType = new HashMap<>();

        try (Stream<FrameIndexEntry> s = stream(sessionDir)) {
            Iterator<FrameIndexEntry> it = s.iterator();
            while (it.hasNext()) {
                FrameIndexEntry e = it.next();
                count++;
                if (e.timestampNanos() < firstTs) firstTs = e.timestampNanos();
                if (e.timestampNanos() > lastTs)  lastTs  = e.timestampNanos();
                totalBytes += e.length();
                byType.merge(e.frameType(), 1L, Long::sum);
            }
        }

        if (count == 0) {
            firstTs = 0; lastTs = 0;
        }
        long durationMillis = (lastTs > firstTs) ? (lastTs - firstTs) / 1_000_000L : 0L;

        return SessionStats.builder()
                .sessionId(sessionId)
                .frameCount(count)
                .firstTimestampNanos(firstTs)
                .lastTimestampNanos(lastTs)
                .durationMillis(durationMillis)
                .totalFrameBytes(totalBytes)
                .framesByType(byType)
                .build();
    }

    /**
     * Convenience adapter to expose {@link Iterator} as a closeable Stream.
     * Useful for callers that need to iterate without manual try-with-resources.
     */
    public static <T> Stream<T> asStream(Iterator<T> it) {
        return StreamSupport.stream(
                Spliterators.spliteratorUnknownSize(it, Spliterator.ORDERED), false);
    }

    private FrameIndexEntry parseLine(String line) {
        if (line == null || line.isBlank()) return null;
        try {
            JsonNode n = mapper.readTree(line);
            return new FrameIndexEntry(
                    n.path("timestampNanos").asLong(),
                    n.path("offset").asLong(),
                    n.path("length").asInt(),
                    n.path("frameType").asInt()
            );
        } catch (IOException e) {
            log.debug("[REPLAY] skipping malformed idx line: {}", e.getMessage());
            return null;
        }
    }
}
