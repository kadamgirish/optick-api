package com.dhan.ticker.service;

import com.dhan.ticker.model.IndexInstrument;
import jakarta.annotation.PostConstruct;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.time.Duration;
import java.time.LocalDate;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

@Slf4j
@Service
public class DhanMasterDataService {

    @Value("${dhan.master-csv-url}")
    private String masterCsvUrl;

    @Value("${dhan.master-csv-cache-dir:cache}")
    private String cacheDir;

    @Value("${dhan.access-token}")
    private String accessToken;

    @Value("${dhan.client-id}")
    private String clientId;

    private final List<IndexInstrument> indices = Collections.synchronizedList(new ArrayList<>());
    private final Map<String, IndexInstrument> instrumentBySecurityId = new ConcurrentHashMap<>();

    private static final Map<String, String> EXCHANGE_SEGMENT_MAP = Map.of(
            "IDX_I", "IDX_I",
            "NSE_EQ", "NSE_EQ",
            "NSE_FNO", "NSE_FNO",
            "BSE_EQ", "BSE_EQ",
            "MCX_COMM", "MCX_COMM"
    );

    private static final Map<String, String> INDEX_CONSTITUENT_URLS = Map.ofEntries(
            Map.entry("NIFTY", "https://archives.nseindia.com/content/indices/ind_nifty50list.csv"),
            Map.entry("BANKNIFTY", "https://archives.nseindia.com/content/indices/ind_niftybanklist.csv"),
            Map.entry("FINNIFTY", "https://archives.nseindia.com/content/indices/ind_niftyfinancelist.csv"),
            Map.entry("NIFTYIT", "https://archives.nseindia.com/content/indices/ind_niftyitlist.csv"),
            Map.entry("NIFTYNEXT50", "https://archives.nseindia.com/content/indices/ind_niftynext50list.csv"),
            Map.entry("MIDCPNIFTY", "https://archives.nseindia.com/content/indices/ind_niftymidcap50list.csv")
    );

    @PostConstruct
    public void init() {
        loadMasterData(false);
    }

    @Scheduled(cron = "0 0 8 * * MON-FRI")
    public void scheduledReload() {
        log.info("Scheduled reload of master instrument data");
        loadMasterData(true);
    }

    public void loadMasterData() {
        loadMasterData(false);
    }

    public void loadMasterData(boolean forceDownload) {
        indices.clear();
        instrumentBySecurityId.clear();

        try {
            Path csvPath = ensureCsvFile(forceDownload);
            if (csvPath == null) {
                log.error("Failed to obtain master CSV file");
                return;
            }

            try (BufferedReader reader = Files.newBufferedReader(csvPath)) {
                String headerLine = reader.readLine();
                if (headerLine == null) {
                    log.error("Master CSV is empty");
                    return;
                }

                String[] headers = headerLine.split(",", -1);
                int secIdIdx = findColumnIndex(headers, "SEM_SMST_SECURITY_ID");
                int exchIdx = findColumnIndex(headers, "SEM_EXM_EXCH_ID");
                int segmentIdx = findColumnIndex(headers, "SEM_SEGMENT");
                int instrumentIdx = findColumnIndex(headers, "SEM_INSTRUMENT_NAME");
                int symbolIdx = findColumnIndex(headers, "SM_SYMBOL_NAME");
                int displayIdx = findColumnIndex(headers, "SEM_CUSTOM_SYMBOL");
                int tradingSymbolIdx = findColumnIndex(headers, "SEM_TRADING_SYMBOL");
                int expiryIdx = findColumnIndex(headers, "SEM_EXPIRY_DATE");
                int strikeIdx = findColumnIndex(headers, "SEM_STRIKE_PRICE");
                int optionTypeIdx = findColumnIndex(headers, "SEM_OPTION_TYPE");

                if (secIdIdx < 0 || instrumentIdx < 0) {
                    log.error("Required columns not found in CSV. Headers: {}", headerLine);
                    return;
                }

                String line;
                int totalCount = 0;
                while ((line = reader.readLine()) != null) {
                    totalCount++;
                    String[] cols = line.split(",", -1);
                    if (cols.length <= Math.max(secIdIdx, instrumentIdx)) {
                        continue;
                    }

                    String instrument = cols[instrumentIdx].trim();
                    String securityId = cols[secIdIdx].trim();
                    String exchange = exchIdx >= 0 && cols.length > exchIdx ? cols[exchIdx].trim() : "";
                    String segment = segmentIdx >= 0 && cols.length > segmentIdx ? cols[segmentIdx].trim() : "";
                    String symbol = symbolIdx >= 0 && cols.length > symbolIdx ? cols[symbolIdx].trim() : "";
                    String display = displayIdx >= 0 && cols.length > displayIdx ? cols[displayIdx].trim() : symbol;
                    String tradingSymbol = tradingSymbolIdx >= 0 && cols.length > tradingSymbolIdx
                            ? cols[tradingSymbolIdx].trim() : "";

                    String exchangeSegment = resolveExchangeSegment(exchange, segment, instrument);

                    String expiry = expiryIdx >= 0 && cols.length > expiryIdx ? cols[expiryIdx].trim() : null;
                    String strike = strikeIdx >= 0 && cols.length > strikeIdx ? cols[strikeIdx].trim() : null;
                    String optType = optionTypeIdx >= 0 && cols.length > optionTypeIdx ? cols[optionTypeIdx].trim() : null;

                    // For F&O, SM_SYMBOL_NAME is empty. Extract base symbol from trading symbol
                    // e.g. "NIFTY-Jun2026-FUT" → "NIFTY", "BANKNIFTY-Jun2026-65400-CE" → "BANKNIFTY"
                    String resolvedSymbol = symbol;
                    if (resolvedSymbol.isEmpty() && !tradingSymbol.isEmpty()) {
                        int dashIdx = tradingSymbol.indexOf('-');
                        resolvedSymbol = dashIdx > 0 ? tradingSymbol.substring(0, dashIdx) : tradingSymbol;
                    }

                    IndexInstrument inst = IndexInstrument.builder()
                            .securityId(securityId)
                            .symbol(resolvedSymbol)
                            .exchangeSegment(exchangeSegment)
                            .instrumentType(instrument)
                            .displayName(display.isEmpty() ? resolvedSymbol : display)
                            .expiryDate(expiry)
                            .strikePrice(strike)
                            .optionType(optType)
                            .tradingSymbol(tradingSymbol)
                            .build();

                    String compositeKey = exchangeSegment + ":" + securityId;
                    instrumentBySecurityId.put(compositeKey, inst);

                    if ("INDEX".equalsIgnoreCase(instrument)) {
                        indices.add(inst);
                    }
                }

                log.info("Master data loaded. Total instruments: {}, INDEX instruments: {}",
                        totalCount, indices.size());
            }
        } catch (Exception e) {
            log.error("Error loading master data: {}", e.getMessage(), e);
        }
    }

    private Path ensureCsvFile(boolean forceDownload) {
        try {
            Path dir = Path.of(cacheDir);
            Files.createDirectories(dir);

            String today = LocalDate.now().toString();
            Path cachedFile = dir.resolve("api-scrip-master-" + today + ".csv");

            // Clean up old dated CSV files
            try (var files = Files.list(dir)) {
                files.filter(p -> p.getFileName().toString().startsWith("api-scrip-master-")
                                && p.getFileName().toString().endsWith(".csv")
                                && !p.getFileName().toString().equals(cachedFile.getFileName().toString()))
                        .forEach(p -> {
                            try {
                                Files.deleteIfExists(p);
                                log.info("Deleted old cache file: {}", p.getFileName());
                            } catch (Exception e) {
                                log.warn("Could not delete old cache: {}", p.getFileName());
                            }
                        });
            }

            if (!forceDownload && Files.exists(cachedFile) && Files.size(cachedFile) > 0) {
                log.info("Using cached CSV from: {}", cachedFile);
                return cachedFile;
            }

            log.info("Downloading master CSV from: {}", masterCsvUrl);
            HttpClient client = HttpClient.newBuilder()
                    .connectTimeout(Duration.ofSeconds(30))
                    .build();
            HttpRequest request = HttpRequest.newBuilder()
                    .uri(URI.create(masterCsvUrl))
                    .timeout(Duration.ofSeconds(120))
                    .GET()
                    .build();

            HttpResponse<java.io.InputStream> response = client.send(request,
                    HttpResponse.BodyHandlers.ofInputStream());

            if (response.statusCode() != 200) {
                log.error("Failed to download master CSV. HTTP status: {}", response.statusCode());
                // Fall back to existing cache if available
                if (Files.exists(cachedFile) && Files.size(cachedFile) > 0) {
                    log.warn("Falling back to existing cached CSV");
                    return cachedFile;
                }
                return null;
            }

            try (var body = response.body()) {
                Files.copy(body, cachedFile, StandardCopyOption.REPLACE_EXISTING);
            }
            log.info("Saved master CSV to: {} ({}MB)", cachedFile,
                    Files.size(cachedFile) / (1024 * 1024));
            return cachedFile;

        } catch (Exception e) {
            log.error("Error ensuring CSV file: {}", e.getMessage(), e);
            // Try to find any existing cache file as fallback
            try {
                Path dir = Path.of(cacheDir);
                if (Files.exists(dir)) {
                    try (var files = Files.list(dir)) {
                        return files.filter(p -> p.getFileName().toString().startsWith("api-scrip-master-")
                                        && p.getFileName().toString().endsWith(".csv"))
                                .findFirst().orElse(null);
                    }
                }
            } catch (Exception ex) {
                log.error("No fallback cache available");
            }
            return null;
        }
    }

    public List<IndexInstrument> getAllIndices() {
        return Collections.unmodifiableList(indices);
    }

    public Optional<IndexInstrument> findBySecurityId(String securityId) {
        // Search all exchange segments for this securityId
        return instrumentBySecurityId.entrySet().stream()
                .filter(e -> e.getKey().endsWith(":" + securityId))
                .map(Map.Entry::getValue)
                .findFirst();
    }

    public Optional<IndexInstrument> findBySecurityId(String exchangeSegment, String securityId) {
        return Optional.ofNullable(instrumentBySecurityId.get(exchangeSegment + ":" + securityId));
    }

    private String resolveExchangeSegment(String exchange, String segment, String instrument) {
        if ("INDEX".equalsIgnoreCase(instrument)) {
            return "IDX_I";
        }
        if ("NSE".equalsIgnoreCase(exchange)) {
            return switch (segment.toUpperCase()) {
                case "E" -> "NSE_EQ";
                case "D" -> "NSE_FNO";
                case "C" -> "NSE_CURRENCY";
                default -> "NSE_EQ";
            };
        }
        if ("BSE".equalsIgnoreCase(exchange)) {
            return switch (segment.toUpperCase()) {
                case "E" -> "BSE_EQ";
                case "D" -> "BSE_FNO";
                case "C" -> "BSE_CURRENCY";
                default -> "BSE_EQ";
            };
        }
        if ("MCX".equalsIgnoreCase(exchange)) {
            return "MCX_COMM";
        }
        return "NSE_EQ";
    }

    private int findColumnIndex(String[] headers, String columnName) {
        for (int i = 0; i < headers.length; i++) {
            if (headers[i].trim().equalsIgnoreCase(columnName)) {
                return i;
            }
        }
        return -1;
    }

    // ── F&O Search ────────────────────────────────────────────────────────

    public List<IndexInstrument> searchFnO(String symbol, String instrumentType,
                                           String optionType, String strike, int limit) {
        LocalDate today = LocalDate.now();

        List<IndexInstrument> fnoList = instrumentBySecurityId.values().stream()
                .filter(i -> isFnO(i.getInstrumentType()))
                .filter(i -> symbol.equalsIgnoreCase(i.getSymbol()))
                .filter(i -> instrumentType == null || instrumentType.equalsIgnoreCase(i.getInstrumentType()))
                .filter(i -> optionType == null || optionType.equalsIgnoreCase(i.getOptionType()))
                .filter(i -> strike == null || matchStrike(strike, i.getStrikePrice()))
                .filter(i -> isActiveFuture(i.getExpiryDate(), today))
                .filter(i -> !"XX".equalsIgnoreCase(i.getOptionType()) || i.getInstrumentType().startsWith("FUT"))
                .toList();

        // Default: filter to nearest expiry per category (futures / options)
        Set<String> nearestExpiries = new HashSet<>();
        fnoList.stream()
                .filter(i -> i.getInstrumentType().startsWith("FUT"))
                .map(IndexInstrument::getExpiryDate)
                .min(Comparator.comparing(this::parseDate))
                .ifPresent(nearestExpiries::add);
        fnoList.stream()
                .filter(i -> i.getInstrumentType().startsWith("OPT"))
                .map(IndexInstrument::getExpiryDate)
                .min(Comparator.comparing(this::parseDate))
                .ifPresent(nearestExpiries::add);

        return fnoList.stream()
                .filter(i -> nearestExpiries.contains(i.getExpiryDate()))
                .sorted(Comparator.comparing((IndexInstrument i) -> i.getInstrumentType().startsWith("FUT") ? 0 : 1)
                        .thenComparing(i -> parseStrike(i.getStrikePrice()))
                        .thenComparing(i -> i.getOptionType() != null ? i.getOptionType() : ""))
                .limit(limit)
                .toList();
    }

    public List<String> getExpiries(String symbol, String instrumentType) {
        LocalDate today = LocalDate.now();
        return instrumentBySecurityId.values().stream()
                .filter(i -> isFnO(i.getInstrumentType()))
                .filter(i -> symbol.equalsIgnoreCase(i.getSymbol()))
                .filter(i -> instrumentType == null || instrumentType.equalsIgnoreCase(i.getInstrumentType()))
                .filter(i -> isActiveFuture(i.getExpiryDate(), today))
                .map(IndexInstrument::getExpiryDate)
                .distinct()
                .sorted(Comparator.comparing(this::parseDate))
                .toList();
    }

    private boolean isFnO(String type) {
        if (type == null) return false;
        return type.startsWith("FUT") || type.startsWith("OPT");
    }

    private boolean isActiveFuture(String expiryDate, LocalDate today) {
        if (expiryDate == null || expiryDate.isBlank()) return false;
        LocalDate exp = parseDate(expiryDate);
        return exp != LocalDate.MAX && !exp.isBefore(today);
    }

    private boolean matchStrike(String target, String actual) {
        if (actual == null) return false;
        try {
            double t = Double.parseDouble(target);
            double a = Double.parseDouble(actual);
            return Math.abs(t - a) < 0.01;
        } catch (Exception e) {
            return target.equals(actual);
        }
    }

    private LocalDate parseDate(String dateStr) {
        if (dateStr == null || dateStr.isBlank()) return LocalDate.MAX;
        try {
            return LocalDate.parse(dateStr.substring(0, Math.min(10, dateStr.length())));
        } catch (Exception e) {
            return LocalDate.MAX;
        }
    }

    private double parseStrike(String strike) {
        if (strike == null || strike.isBlank()) return 0;
        try { return Double.parseDouble(strike); }
        catch (Exception e) { return Double.MAX_VALUE; }
    }

    // ── One-click chain builder ────────────────────────────────────────

    public Optional<IndexInstrument> findIndexBySymbol(String symbol) {
        return indices.stream()
                .filter(i -> symbol.equalsIgnoreCase(i.getSymbol()))
                .findFirst();
    }

    /**
     * Builds complete chain: spot + nearest future + N CE/PE around ATM.
     */
    public ChainResult buildChain(String symbol, Double spotPrice, int strikesPerSide) {
        LocalDate today = LocalDate.now();

        // 1. Spot index
        IndexInstrument spotInst = findIndexBySymbol(symbol).orElse(null);

        // 2. All active F&O for this symbol
        List<IndexInstrument> allFnO = instrumentBySecurityId.values().stream()
                .filter(i -> isFnO(i.getInstrumentType()))
                .filter(i -> symbol.equalsIgnoreCase(i.getSymbol()))
                .filter(i -> isActiveFuture(i.getExpiryDate(), today))
                .filter(i -> !"XX".equalsIgnoreCase(i.getOptionType()) || i.getInstrumentType().startsWith("FUT"))
                .toList();

        // 3. Nearest future
        String nearestFutExpiry = allFnO.stream()
                .filter(i -> i.getInstrumentType().startsWith("FUT"))
                .map(IndexInstrument::getExpiryDate)
                .min(Comparator.comparing(this::parseDate))
                .orElse(null);

        IndexInstrument nearestFut = nearestFutExpiry == null ? null :
                allFnO.stream()
                        .filter(i -> i.getInstrumentType().startsWith("FUT"))
                        .filter(i -> nearestFutExpiry.equals(i.getExpiryDate()))
                        .findFirst().orElse(null);

        // 4. Nearest option expiry
        String nearestOptExpiry = allFnO.stream()
                .filter(i -> i.getInstrumentType().startsWith("OPT"))
                .map(IndexInstrument::getExpiryDate)
                .min(Comparator.comparing(this::parseDate))
                .orElse(null);

        List<IndexInstrument> nearestOptions = nearestOptExpiry == null ? List.of() :
                allFnO.stream()
                        .filter(i -> i.getInstrumentType().startsWith("OPT"))
                        .filter(i -> nearestOptExpiry.equals(i.getExpiryDate()))
                        .toList();

        // 5. Determine ATM reference price — use live LTP if available
        double refPrice;
        if (spotPrice != null) {
            refPrice = spotPrice;
        } else if (spotInst != null) {
            double liveLtp = fetchSpotLtp(spotInst.getExchangeSegment(), spotInst.getSecurityId());
            refPrice = liveLtp > 0 ? liveLtp : guessAtmPrice(nearestOptions);
            if (liveLtp > 0) log.info("Using live LTP {} as ATM reference for {}", liveLtp, symbol);
        } else {
            refPrice = guessAtmPrice(nearestOptions);
        }

        // 6. Pick N CE + N PE closest to ATM
        List<IndexInstrument> selectedCE = nearestOptions.stream()
                .filter(i -> "CE".equalsIgnoreCase(i.getOptionType()))
                .sorted(Comparator.comparingDouble(i -> Math.abs(parseStrike(i.getStrikePrice()) - refPrice)))
                .limit(strikesPerSide)
                .sorted(Comparator.comparingDouble(i -> parseStrike(i.getStrikePrice())))
                .toList();

        List<IndexInstrument> selectedPE = nearestOptions.stream()
                .filter(i -> "PE".equalsIgnoreCase(i.getOptionType()))
                .sorted(Comparator.comparingDouble(i -> Math.abs(parseStrike(i.getStrikePrice()) - refPrice)))
                .limit(strikesPerSide)
                .sorted(Comparator.comparingDouble(i -> parseStrike(i.getStrikePrice())))
                .toList();

        return new ChainResult(spotInst, nearestFut, nearestFutExpiry, nearestOptExpiry,
                selectedCE, selectedPE, refPrice);
    }

    /**
     * Fetch live LTP from Dhan REST API for a single instrument.
     */
    public double fetchSpotLtp(String exchangeSegment, String securityId) {
        try {
            String body = "{\"" + exchangeSegment + "\":[" + securityId + "]}";
            HttpClient client = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(10)).build();
            HttpRequest request = HttpRequest.newBuilder()
                    .uri(URI.create("https://api.dhan.co/v2/marketfeed/ltp"))
                    .timeout(Duration.ofSeconds(10))
                    .header("Content-Type", "application/json")
                    .header("Accept", "application/json")
                    .header("access-token", accessToken)
                    .header("client-id", clientId)
                    .POST(HttpRequest.BodyPublishers.ofString(body))
                    .build();
            HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());
            if (response.statusCode() == 200) {
                String json = response.body();
                // Parse: {"data":{"IDX_I":{"13":{"last_price":24436.35}}},"status":"success"}
                String key = "\"last_price\":";
                int idx = json.indexOf(key);
                if (idx >= 0) {
                    int start = idx + key.length();
                    int end = json.indexOf('}', start);
                    String val = json.substring(start, end).trim();
                    double ltp = Double.parseDouble(val);
                    log.info("Fetched LTP for {}:{} = {}", exchangeSegment, securityId, ltp);
                    return ltp;
                }
            } else {
                log.warn("LTP API returned HTTP {}: {}", response.statusCode(), response.body());
            }
        } catch (Exception e) {
            log.warn("Failed to fetch LTP for {}:{} — {}", exchangeSegment, securityId, e.getMessage());
        }
        return 0;
    }

    private double guessAtmPrice(List<IndexInstrument> options) {
        List<Double> strikes = options.stream()
                .map(i -> parseStrike(i.getStrikePrice()))
                .filter(s -> s > 0 && s < Double.MAX_VALUE)
                .distinct().sorted().toList();
        if (strikes.isEmpty()) return 0;
        return strikes.get(strikes.size() / 2);
    }

    /**
     * OI snapshot from Option Chain API: previous day OI + current OI per security.
     */
    public record OiSnapshot(Map<String, Long> previousOi, Map<String, Long> currentOi) {}

    /**
     * Fetch previous day OI AND current OI for all options via Dhan Option Chain API.
     * Returns OiSnapshot with both maps keyed by securityId.
     */
    public OiSnapshot fetchOptionChainOi(String spotSecurityId, String spotSegment, String expiry) {
        Map<String, Long> prevResult = new HashMap<>();
        Map<String, Long> currResult = new HashMap<>();
        try {
            String expiryDate = expiry.substring(0, Math.min(10, expiry.length()));
            String body = String.format(
                    "{\"UnderlyingScrip\":%s,\"UnderlyingSeg\":\"%s\",\"Expiry\":\"%s\"}",
                    spotSecurityId, spotSegment, expiryDate);

            HttpClient client = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(10)).build();
            HttpRequest request = HttpRequest.newBuilder()
                    .uri(URI.create("https://api.dhan.co/v2/optionchain"))
                    .timeout(Duration.ofSeconds(15))
                    .header("Content-Type", "application/json")
                    .header("Accept", "application/json")
                    .header("access-token", accessToken)
                    .header("client-id", clientId)
                    .POST(HttpRequest.BodyPublishers.ofString(body))
                    .build();

            HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());
            if (response.statusCode() == 200) {
                String json = response.body();
                // JSON keys are alphabetical per CE/PE block:
                //   "oi":..., "previous_close_price":..., "previous_oi":..., "previous_volume":..., "security_id":...
                // We find "previous_oi" first (it comes before "security_id"),
                // then backtrack to find "oi" for the same block (between previous block boundary and "previous_oi").
                int searchFrom = 0;
                while (true) {
                    int prevOiIdx = json.indexOf("\"previous_oi\"", searchFrom);
                    if (prevOiIdx < 0) break;
                    int secIdx = json.indexOf("\"security_id\"", prevOiIdx);
                    int nextPrevOiIdx = json.indexOf("\"previous_oi\"", prevOiIdx + 13);
                    if (secIdx < 0 || (nextPrevOiIdx > 0 && secIdx > nextPrevOiIdx)) {
                        searchFrom = prevOiIdx + 13;
                        continue;
                    }
                    // Extract previous_oi value
                    int oiColon = json.indexOf(':', prevOiIdx + 13);
                    int oiEnd = json.indexOf(',', oiColon);
                    if (oiEnd < 0) oiEnd = json.indexOf('}', oiColon);
                    String prevOiVal = json.substring(oiColon + 1, oiEnd).trim();
                    // Extract security_id value
                    int colonIdx = json.indexOf(':', secIdx + 13);
                    int commaIdx = json.indexOf(',', colonIdx);
                    String secId = json.substring(colonIdx + 1, commaIdx).trim();
                    // Extract current "oi" — search backwards from prevOiIdx for nearest "oi":
                    // Find the last occurrence of "oi": before prevOiIdx that is NOT part of "previous_oi"
                    long currOi = 0;
                    int oiSearchEnd = prevOiIdx;
                    // Look for standalone "oi" key between the CE/PE block start and "previous_oi"
                    int blockStart = json.lastIndexOf('{', prevOiIdx);
                    if (blockStart >= 0) {
                        int oiKeyIdx = json.indexOf("\"oi\"", blockStart);
                        // Make sure this "oi" is before "previous_oi" and is standalone (not part of "previous_oi")
                        if (oiKeyIdx >= 0 && oiKeyIdx < prevOiIdx) {
                            // Verify it's standalone: char before '"' should not be '_'
                            if (oiKeyIdx == 0 || json.charAt(oiKeyIdx - 1) != '_') {
                                int curOiColon = json.indexOf(':', oiKeyIdx + 4);
                                int curOiEnd = json.indexOf(',', curOiColon);
                                if (curOiEnd < 0) curOiEnd = json.indexOf('}', curOiColon);
                                String curOiVal = json.substring(curOiColon + 1, curOiEnd).trim();
                                try { currOi = Long.parseLong(curOiVal); } catch (NumberFormatException ignored) {}
                            }
                        }
                    }
                    try {
                        long prevOi = Long.parseLong(prevOiVal);
                        if (prevOi > 0) prevResult.put(secId, prevOi);
                    } catch (NumberFormatException ignored) {}
                    if (currOi > 0) currResult.put(secId, currOi);
                    searchFrom = secIdx + 13;
                }
                log.info("Fetched OI for {} option strikes from Option Chain API (prevOI={}, currentOI={})",
                        Math.max(prevResult.size(), currResult.size()), prevResult.size(), currResult.size());
            } else {
                log.warn("Option Chain API returned HTTP {}: {}", response.statusCode(),
                        response.body().substring(0, Math.min(200, response.body().length())));
            }
        } catch (Exception e) {
            log.warn("Failed to fetch Option Chain OI: {}", e.getMessage());
        }
        return new OiSnapshot(prevResult, currResult);
    }

    /**
     * Fetch current OI for a future instrument via Dhan Market Quote API.
     * Returns the OI value, or 0 if not available.
     */
    public long fetchFutureOi(String exchangeSegment, String securityId) {
        try {
            String body = "{\"" + exchangeSegment + "\":[" + securityId + "]}";
            HttpClient client = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(10)).build();
            HttpRequest request = HttpRequest.newBuilder()
                    .uri(URI.create("https://api.dhan.co/v2/marketfeed/quote"))
                    .timeout(Duration.ofSeconds(10))
                    .header("Content-Type", "application/json")
                    .header("Accept", "application/json")
                    .header("access-token", accessToken)
                    .header("client-id", clientId)
                    .POST(HttpRequest.BodyPublishers.ofString(body))
                    .build();
            HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());
            if (response.statusCode() == 200) {
                String json = response.body();
                // Parse "oi": value — must be standalone, not "previous_oi" or "oi_day_high"
                String key = "\"oi\":";
                int idx = 0;
                while ((idx = json.indexOf(key, idx)) >= 0) {
                    // Check it's standalone: char before '"' should not be '_' and char after value colon context
                    if (idx == 0 || json.charAt(idx - 1) != '_') {
                        int start = idx + key.length();
                        int end = json.indexOf(',', start);
                        if (end < 0) end = json.indexOf('}', start);
                        String val = json.substring(start, end).trim();
                        long oi = Long.parseLong(val);
                        if (oi > 0) {
                            log.info("Fetched future OI for {}:{} = {}", exchangeSegment, securityId, oi);
                            return oi;
                        }
                    }
                    idx += key.length();
                }
            } else {
                log.warn("Market Quote API returned HTTP {}: {}", response.statusCode(), response.body());
            }
        } catch (Exception e) {
            log.warn("Failed to fetch future OI for {}:{} — {}", exchangeSegment, securityId, e.getMessage());
        }
        return 0;
    }

    public record ChainResult(
            IndexInstrument spot,
            IndexInstrument nearestFuture,
            String futureExpiry,
            String optionExpiry,
            List<IndexInstrument> callOptions,
            List<IndexInstrument> putOptions,
            double referencePrice
    ) {}

    // ── Index constituent stocks ────────────────────────────────────────

    public List<IndexInstrument> getIndexConstituents(String indexSymbol) {
        String url = INDEX_CONSTITUENT_URLS.get(indexSymbol.toUpperCase());
        if (url == null) {
            log.warn("No constituent URL configured for index: {}", indexSymbol);
            return List.of();
        }

        try {
            Path dir = Path.of(cacheDir);
            Files.createDirectories(dir);
            String today = LocalDate.now().toString();
            String fileName = "constituents-" + indexSymbol.toLowerCase() + "-" + today + ".csv";
            Path cachedFile = dir.resolve(fileName);

            // Clean old constituent files for this index
            try (var files = Files.list(dir)) {
                files.filter(p -> p.getFileName().toString().startsWith("constituents-" + indexSymbol.toLowerCase() + "-")
                                && !p.getFileName().toString().equals(fileName))
                        .forEach(p -> { try { Files.deleteIfExists(p); } catch (Exception e) { /* ignore */ } });
            }

            if (!Files.exists(cachedFile) || Files.size(cachedFile) == 0) {
                log.info("Downloading {} constituents from: {}", indexSymbol, url);
                HttpClient client = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(15)).build();
                HttpRequest request = HttpRequest.newBuilder()
                        .uri(URI.create(url))
                        .timeout(Duration.ofSeconds(30))
                        .header("User-Agent", "Mozilla/5.0")
                        .GET().build();
                HttpResponse<java.io.InputStream> response = client.send(request,
                        HttpResponse.BodyHandlers.ofInputStream());
                if (response.statusCode() != 200) {
                    log.error("Failed to download {} constituents. HTTP {}", indexSymbol, response.statusCode());
                    return List.of();
                }
                try (var body = response.body()) {
                    Files.copy(body, cachedFile, StandardCopyOption.REPLACE_EXISTING);
                }
                log.info("Saved {} constituents to: {}", indexSymbol, cachedFile);
            } else {
                log.info("Using cached {} constituents from: {}", indexSymbol, cachedFile);
            }

            // Parse stock symbols from NSE CSV (has "Symbol" column)
            List<String> stockSymbols = new ArrayList<>();
            try (BufferedReader reader = Files.newBufferedReader(cachedFile)) {
                String header = reader.readLine();
                if (header == null) return List.of();
                String[] cols = header.split(",", -1);
                int symbolIdx = -1;
                for (int i = 0; i < cols.length; i++) {
                    if (cols[i].trim().equalsIgnoreCase("Symbol")) { symbolIdx = i; break; }
                }
                if (symbolIdx < 0) return List.of();

                String line;
                while ((line = reader.readLine()) != null) {
                    String[] parts = line.split(",", -1);
                    if (parts.length > symbolIdx) {
                        String sym = parts[symbolIdx].trim();
                        if (!sym.isEmpty()) stockSymbols.add(sym);
                    }
                }
            }

            log.info("Found {} constituent symbols for {}", stockSymbols.size(), indexSymbol);
            return findEquitiesBySymbols(stockSymbols);

        } catch (Exception e) {
            log.error("Error loading constituents for {}: {}", indexSymbol, e.getMessage());
            return List.of();
        }
    }

    public List<IndexInstrument> findEquitiesBySymbols(List<String> symbols) {
        Set<String> symbolSet = symbols.stream()
                .map(String::toUpperCase)
                .collect(Collectors.toSet());

        // NSE CSV uses trading symbol (e.g. "RELIANCE"), Dhan SM_SYMBOL_NAME
        // is full name (e.g. "RELIANCE INDUSTRIES LTD"), so match on tradingSymbol
        Map<String, IndexInstrument> result = new LinkedHashMap<>();
        instrumentBySecurityId.values().stream()
                .filter(i -> "NSE_EQ".equals(i.getExchangeSegment()))
                .filter(i -> symbolSet.contains(i.getTradingSymbol().toUpperCase()))
                .forEach(i -> result.putIfAbsent(i.getTradingSymbol().toUpperCase(), i));

        Set<String> unmatched = new HashSet<>(symbolSet);
        unmatched.removeAll(result.keySet());
        if (!unmatched.isEmpty()) {
            log.warn("Unmatched constituent symbols ({}): {}", unmatched.size(), unmatched);
        }
        log.info("Matched {} equities out of {} requested symbols", result.size(), symbolSet.size());
        return new ArrayList<>(result.values());
    }
}
