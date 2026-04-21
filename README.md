# Optick API

Live options tick data, OI tracking, and multi-index subscription backend built on the Dhan WebSocket market feed.

## Features

- Connects to Dhan WebSocket (`wss://api-feed.dhan.co`) and streams binary ticks
- Subscribes in one call: spot index + nearest future + 20 CE/PE around ATM + constituent stocks
- Supports multiple concurrent index subscriptions (NIFTY, BANKNIFTY, FINNIFTY, etc.) with shared-instrument handling
- Pre-loads previous-day OI baseline from Dhan Option Chain + Market Quote APIs for immediate OI-change on subscribe
- Broadcasts ticks to frontend via STOMP over WebSocket (`/topic/ticks/*`)
- Daily master-data CSV refresh (weekdays at 8am IST) with cache fallback
- Swagger UI for interactive API docs

## Stack

- Java 17, Spring Boot 3.2.5, Maven
- `org.java-websocket` 1.5.6 for Dhan binary feed
- SpringDoc OpenAPI 2.5.0 for Swagger UI
- OpenCSV 5.9 for master-data parsing
- STOMP over WebSocket for frontend broadcast

## Configuration

Edit [src/main/resources/application.yml](src/main/resources/application.yml):

```yaml
dhan:
  access-token: <your-dhan-access-token>
  client-id: <your-dhan-client-id>
  websocket-url: wss://api-feed.dhan.co
  master-csv-url: https://images.dhan.co/api-data/api-scrip-master.csv
  master-csv-cache-dir: cache
```

## Build & Run

```bash
mvn package -DskipTests -q
lsof -ti:8080 | xargs kill -9 2>/dev/null
java -Xmx512m -Xms128m -jar target/optick-api-1.0.0.jar
```

**Important:** Must use 512 MB+ heap — ~262K instruments are loaded into memory at startup. 256 MB triggers OOM kill.

Do **not** use `mvn spring-boot:run` — Maven's JVM does not honor the heap flags and gets OOM-killed (exit 137).

- API base URL: http://localhost:8080
- Swagger UI: http://localhost:8080/swagger-ui.html

## REST API

### Master data

| Method | Path                                                              | Purpose                                                       |
| ------ | ----------------------------------------------------------------- | ------------------------------------------------------------- |
| GET    | `/api/indices`                                                    | List all INDEX instruments (~194)                             |
| GET    | `/api/instruments/chain?symbol=&spotPrice=&strikes=10`            | Spot + future + CE/PE chain with ready-to-use connect payload |
| GET    | `/api/instruments/fno?symbol=&type=&optionType=&strike=&limit=50` | F&O search with filters                                       |
| GET    | `/api/instruments/expiries?symbol=&type=`                         | Available expiry dates                                        |

### Subscription management

| Method | Path                                            | Purpose                                                                                             |
| ------ | ----------------------------------------------- | --------------------------------------------------------------------------------------------------- |
| POST   | `/api/instruments/subscribe?symbol=`            | One-click subscribe: spot + future + 20 CE/PE + constituent stocks                                  |
| POST   | `/api/instruments/unsubscribe?symbol=`          | Unsubscribe an index (keeps stocks shared with other active groups)                                 |
| GET    | `/api/instruments/subscribed-groups`            | Active index subscriptions with instrument counts                                                   |
| GET    | `/api/instruments/subscribed-by-index/{symbol}` | Full instrument list for a specific subscribed index (use on page reload to restore frontend state) |

### Low-level WebSocket control

| Method | Path                  | Purpose                                    |
| ------ | --------------------- | ------------------------------------------ |
| POST   | `/api/ws/connect`     | Connect & subscribe arbitrary instruments  |
| POST   | `/api/ws/subscribe`   | Add instruments to existing connection     |
| POST   | `/api/ws/unsubscribe` | Remove instruments by securityId list      |
| POST   | `/api/ws/disconnect`  | Disconnect WebSocket                       |
| POST   | `/api/ws/pause`       | Pause tick logging                         |
| POST   | `/api/ws/resume`      | Resume tick logging                        |
| GET    | `/api/ws/status`      | Connection status + subscribed instruments |

## Architecture

```
com.dhan.ticker
├── OptickApplication           # Spring Boot entry point
├── config/
│   ├── CorsConfig              # CORS for frontend dev
│   ├── OpenApiConfig           # Swagger metadata
│   └── WebSocketBrokerConfig   # STOMP broker for frontend broadcast
├── controller/
│   ├── IndicesController
│   ├── InstrumentSearchController
│   └── WebSocketController
├── service/
│   ├── MasterDataService       # CSV load, chain builder, Dhan REST APIs (LTP, OptionChain, Quote)
│   └── MarketFeedService       # WebSocket client, binary-packet parsing, STOMP broadcast, subscription groups
├── model/                      # ApiResponse, ConnectRequest, IndexInstrument, TickData
└── exception/                  # GlobalExceptionHandler + custom exceptions
```

### Subscription group tracking

`MarketFeedService` holds two maps that together enable correct handling of indices with shared constituents (e.g. HDFCBANK in both NIFTY and BANKNIFTY):

- `subscriptionGroups: indexSymbol → Set<"segment:securityId">`
- `subscribedInstruments: "segment:securityId" → IndexInstrument`

Unsubscribing an index removes only securityIds not owned by any other active group. The `GET /subscribed-by-index/{symbol}` endpoint joins these maps so the frontend can build a `securityId → Set<index>` map and display each instrument under every index it belongs to.

## Notes

- Dhan CSV `securityId` values are **not unique across segments** (e.g. secId 25 = ADANIENT equity _and_ BANKNIFTY index). Internal map keys are composite `exchangeSegment:securityId`.
- `IDX_I` instruments only support `TICKER` feed mode — `QUOTE` / `FULL` are silently ignored by Dhan.
- Access token and client ID are stored in `application.yml` (not environment variables) per user preference.
