package com.dhan.ticker.replay;

import com.dhan.ticker.exception.WebSocketException;
import com.dhan.ticker.model.replay.SessionManifest;
import com.dhan.ticker.model.replay.SessionSummary;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.annotation.PostConstruct;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Stream;

/**
 * Discovers recorded replay sessions on disk and parses their manifests.
 * Each session is a single sub-directory of {@code replay.data-dir} containing
 * {@code manifest.json + frames.idx.ndjson + frames.bin}.
 */
@Slf4j
@Service
public class SessionDiscoveryService {

    private final Path dataDir;
    private final ObjectMapper mapper = new ObjectMapper()
            .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
    private final Map<String, SessionManifest> manifestCache = new ConcurrentHashMap<>();

    public SessionDiscoveryService(@Value("${replay.data-dir}") String dataDir) {
        this.dataDir = Path.of(dataDir);
    }

    @PostConstruct
    public void init() {
        if (!Files.isDirectory(dataDir)) {
            log.warn("[REPLAY] data-dir does not exist or is not a directory: {}", dataDir);
        } else {
            log.info("[REPLAY] data-dir = {}", dataDir);
        }
    }

    public Path getDataDir() {
        return dataDir;
    }

    /** All session folders, sorted by startTime descending (newest first). */
    public List<SessionSummary> listSessions() {
        if (!Files.isDirectory(dataDir)) return List.of();
        List<SessionSummary> out = new ArrayList<>();
        try (Stream<Path> children = Files.list(dataDir)) {
            children.filter(Files::isDirectory)
                    .filter(SessionDiscoveryService::looksLikeSession)
                    .forEach(p -> {
                        try {
                            out.add(toSummary(p));
                        } catch (Exception e) {
                            log.warn("[REPLAY] Skipping {}: {}", p.getFileName(), e.getMessage());
                        }
                    });
        } catch (IOException e) {
            log.error("[REPLAY] Failed to list sessions: {}", e.getMessage());
            return List.of();
        }
        out.sort(Comparator.comparing(SessionSummary::getStartTime,
                        Comparator.nullsLast(Comparator.naturalOrder())).reversed());
        return out;
    }

    /** Resolve a sessionId to an on-disk path, validating against directory listing. */
    public Path resolveSessionDir(String sessionId) {
        if (sessionId == null || sessionId.isBlank()) {
            throw new WebSocketException("sessionId is required");
        }
        if (sessionId.contains("/") || sessionId.contains("\\") || sessionId.contains("..")) {
            throw new WebSocketException("Invalid sessionId");
        }
        Path dir = dataDir.resolve(sessionId);
        if (!Files.isDirectory(dir) || !looksLikeSession(dir)) {
            throw new WebSocketException("Session not found: " + sessionId);
        }
        return dir;
    }

    /** Cached manifest read. */
    public SessionManifest getManifest(String sessionId) {
        return manifestCache.computeIfAbsent(sessionId, this::loadManifest);
    }

    private SessionManifest loadManifest(String sessionId) {
        Path manifestFile = resolveSessionDir(sessionId).resolve("manifest.json");
        try {
            return mapper.readValue(manifestFile.toFile(), SessionManifest.class);
        } catch (IOException e) {
            throw new WebSocketException("Failed to read manifest for " + sessionId + ": " + e.getMessage(), e);
        }
    }

    private static boolean looksLikeSession(Path dir) {
        return Files.isRegularFile(dir.resolve("manifest.json"))
                && Files.isRegularFile(dir.resolve("frames.idx.ndjson"))
                && Files.isRegularFile(dir.resolve("frames.bin"));
    }

    private SessionSummary toSummary(Path dir) throws IOException {
        SessionManifest m = mapper.readValue(dir.resolve("manifest.json").toFile(), SessionManifest.class);
        long binSize = Files.size(dir.resolve("frames.bin"));
        long idxSize = Files.size(dir.resolve("frames.idx.ndjson"));
        return SessionSummary.builder()
                .sessionId(m.getSessionId() != null ? m.getSessionId() : dir.getFileName().toString())
                .startTime(m.getStartTime())
                .endTime(m.getEndTime())
                .basketPreset(m.getBasketPreset())
                .feedMode(m.getFeedMode())
                .instrumentCount(m.getInstruments() == null ? 0 : m.getInstruments().size())
                .framesBinBytes(binSize)
                .framesIdxBytes(idxSize)
                .build();
    }
}
