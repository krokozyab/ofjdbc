# Configuration: URL Parameters, Properties and Environment Variables

The driver reads its settings from four places. There are two kinds of settings, and they are read differently:

- **Connection settings** apply to one connection and are looked up, highest first, in the **JDBC URL parameter**
  (`?fetchSize=1000`), the **connection property** (DBeaver: Driver properties), the **system property**
  (`-Dofjdbc.fetchSize=1000`) and the **environment variable** (`OFJDBC_DEFAULT_FETCH_SIZE=1000`); otherwise the default.
- **Driver-wide settings** (HTTP, retries, circuit breaker, browser) apply to the whole JVM and are read once, when the
  driver is first used: from the system property or the environment variable only — restart the application (DBeaver)
  after changing them.

## Contents

- [Connection settings](#connection-settings)
- [Driver-wide settings](#driver-wide-settings)
- [Browser SSO](#browser-sso)
- [OAuth Authentication](#oauth-authentication)
- [DUCK Workspace](#duck-workspace-ofjdbc_duck_db)
- [Next result page cache](#next-result-page-cache-ofjdbc_next_page_cache_seconds)
- [Query history](#query-history-ofjdbc_history)
- [REST schema](#rest-schema)
- [Query Profiler](#query-profiler-ofjdbc_profile)
- [Usage examples](#usage-examples)
- [Troubleshooting](#troubleshooting)

---

## Connection settings

| URL parameter / connection property | System property | Environment variable | Default | Meaning |
|---|---|---|---|---|
| `fetchSize` | `ofjdbc.fetchSize` | `OFJDBC_DEFAULT_FETCH_SIZE` | `500` | Rows per page read from Fusion, `1`–`10000` (larger values are capped with a log warning) |
| `nextPageCacheSeconds` | `ofjdbc.nextPageCacheSeconds` | `OFJDBC_NEXT_PAGE_CACHE_SECONDS` | `300` | How long rows are kept for DBeaver's next result page; `0` off ([below](#next-result-page-cache-ofjdbc_next_page_cache_seconds)) |
| `ofjdbc.prefetch.enabled` | same | `OFJDBC_PREFETCH_ENABLED` | `true` | Read the next page in the background while you look at the current one |
| `ofjdbc.prefetch.threshold` | same | `OFJDBC_PREFETCH_THRESHOLD` | `0.75` | Share of a page read before the next one is requested, `0.0`–`1.0` (`1.0` waits for the whole page) |
| `prefetchMetadata` | `ofjdbc.metadata.prefetchEnabled` | `OFJDBC_METADATA_PREFETCH_ENABLED` | `false` | Fill the metadata cache in the background; can open many BI Publisher sessions, so off by default |
| `profile` | `ofjdbc.profile` | `OFJDBC_PROFILE` | `false` | Page telemetry in the Output panel ([below](#query-profiler-ofjdbc_profile)) |
| `duckDb` | `ofjdbc.duckDb` | `OFJDBC_DUCK_DB` | `~/.ofjdbc/duck.duckdb` | File of the DUCK workspace; `:memory:` for none |
| `duckTransactions` | `ofjdbc.duckTransactions` | `OFJDBC_DUCK_TRANSACTIONS` | `false` | Whether Commit and Rollback apply to DUCK tables ([below](#duck-workspace-ofjdbc_duck_db)) |
| `history` | `ofjdbc.history` | `OFJDBC_HISTORY` | `false` | Keep the rows of every Fusion query ([below](#query-history-ofjdbc_history)) |
| `historyMaxRows` | `ofjdbc.historyMaxRows` | `OFJDBC_HISTORY_MAX_ROWS` | `1000000` | Most rows kept in the history |
| `rest` | `ofjdbc.rest` | `OFJDBC_REST` | `true` | `false` hides the `REST` schema |
| `restApi` | `ofjdbc.restApi` | `OFJDBC_REST_API` | `fscmRestApi` | REST API family |
| `restVersion` | `ofjdbc.restVersion` | `OFJDBC_REST_VERSION` | `latest` | REST API version |
| `restMaxRows` | `ofjdbc.restMaxRows` | `OFJDBC_REST_MAX_ROWS` | `50000` | Most rows one statement may read from a REST resource |
| `restMaxParents` | `ofjdbc.restMaxParents` | `OFJDBC_REST_MAX_PARENTS` | `200` | Most parents a REST child table is read for in one statement |
| `restMaxRequests` | `ofjdbc.restMaxRequests` | `OFJDBC_REST_MAX_REQUESTS` | `2000` | Most REST requests of one statement for child tables or the keys of a join |
| `restMaxKeys` | `ofjdbc.restMaxKeys` | `OFJDBC_REST_MAX_KEYS` | `1000` | Most keys of a DUCK table a join sends with a REST read |
| `authType` | — | — | password | `BROWSER` for SSO, `OAUTH` for OAuth ([below](#oauth-authentication)) |

A connection property may also be written with the `ofjdbc.` prefix (`ofjdbc.fetchSize`), as DBeaver's driver
properties dialog shows it.

**Fetch size.** Smaller pages (100–200) show the first rows sooner; larger ones (1000+) need fewer round trips for
exports. Larger pages use more memory.

---

## Driver-wide settings

### HTTP

| System property | Environment variable | Default | Range | Meaning |
|---|---|---|---|---|
| `ofjdbc.http.connectTimeout` | `OFJDBC_HTTP_CONNECT_TIMEOUT` | `30` s | `1`–`300` | Time to establish the connection |
| `ofjdbc.http.readTimeout` | `OFJDBC_HTTP_READ_TIMEOUT` | `120` s | `5`–`1800` | Time to wait for the answer once connected — raise it for long-running queries |
| `ofjdbc.http.maxConcurrentRequests` | `OFJDBC_MAX_CONCURRENT_REQUESTS` | `1` | `1`–`16` | Requests sent to one Fusion host at the same time |

The system property wins over the environment variable; values outside the range are moved to its nearest end.

### Retries

| System property | Environment variable | Default | Meaning |
|---|---|---|---|
| `ofjdbc.retry.maxAttempts` | `OFJDBC_RETRY_MAX_ATTEMPTS` | `3` | Attempts for a request that failed for a transient reason (timeout, connection reset, HTTP 408, 429, 500, 502, 503, 504) |
| `ofjdbc.retry.baseDelayMs` | `OFJDBC_RETRY_BASE_DELAY_MS` | `1000` | Wait before the first retry |
| `ofjdbc.retry.maxDelayMs` | `OFJDBC_RETRY_MAX_DELAY_MS` | `30000` | Longest wait between retries |
| `ofjdbc.retry.multiplier` | `OFJDBC_RETRY_MULTIPLIER` | `2.0` | Growth of the wait from one retry to the next (with a little random jitter) |

Here the **environment variable wins** over the system property. Errors from Oracle (an ORA- message) and failed
sign-ins are not retried.

### Circuit breaker

After repeated failures in a row the driver stops sending requests to Fusion for a while and fails fast instead, so a
broken connection does not keep you waiting on every statement.

| System property | Environment variable | Default | Range | Meaning |
|---|---|---|---|---|
| `ofjdbc.circuitBreaker.enabled` | `OFJDBC_CIRCUIT_BREAKER_ENABLED` | `true` | | `false` turns it off |
| `ofjdbc.circuitBreaker.failureThreshold` | `OFJDBC_CIRCUIT_BREAKER_THRESHOLD` | `5` | `2`–`100` | Failures in a row that open it |
| `ofjdbc.circuitBreaker.recoveryTimeoutMs` | `OFJDBC_CIRCUIT_BREAKER_TIMEOUT_MS` | `60000` | `5000`–`600000` | How long it stays open before a trial request |
| `ofjdbc.circuitBreaker.halfOpenMaxAttempts` | `OFJDBC_CIRCUIT_BREAKER_HALF_OPEN_ATTEMPTS` | `1` | `1`–`5` | Trial requests before it closes again |

The system property wins over the environment variable.

---

## Browser SSO

| Environment variable / system property | Default | Meaning |
|---|---|---|
| `OFJDBC_CHROME_PATH` | Chrome, then Chromium, then Edge in their default locations | Browser used for `authType=BROWSER`; on Windows Edge is always present, so SSO works without Chrome |
| `OFJDBC_SSO_LOGIN_URL` / `ofjdbc.ssoLoginUrl` | `https://<host>` | Page the browser opens to sign in, if your organisation starts elsewhere |
| `OFJDBC_USE_TEMP_PROFILE` | `false` | `true` uses a throwaway browser profile instead of `~/.ofjdbc/chrome-profile`, so no sign-in cookies are kept between logins |

```bat
setx OFJDBC_CHROME_PATH "C:\Program Files (x86)\Microsoft\Edge\Application\msedge.exe"
```

The persistent profile lets the browser remember your identity provider's session, so the next sign-in is often a
single click. If the browser does not start because one is already running with that profile, close it or set
`OFJDBC_USE_TEMP_PROFILE=true`.

---

## OAuth Authentication

For headless use — ETL jobs, schedulers, services — the driver can authenticate with an OAuth 2.0 access token
instead of a password or a browser login. It requests the token itself (client credentials grant by default),
sends it as `Authorization: Bearer …`, caches it per process (connections with the same client share it), renews
it shortly before it expires and once more when Fusion rejects it.

Enable it with `authType=OAUTH` in the URL or connection properties:

```
jdbc:wsdl://<host>/xmlpserver/services/ExternalReportWSSService?WSDL:/Custom/Financials/RP_ARB.xdo?authType=OAUTH
```

The client id and secret default to the connection's `user` and `password`; everything else is read by the
built-in provider (`my.jdbc.wsdl_driver.SystemEnvOAuthProvider`) from the driver property, the system property of
the same name, or the environment variable:

| Driver / system property      | Environment variable          | Default              | Description |
|-------------------------------|-------------------------------|----------------------|-------------|
| `ofjdbc.oauth.tokenEndpoint`  | `OFJDBC_OAUTH_TOKEN_ENDPOINT` | — (required)         | Token endpoint, e.g. `https://idcs-<id>.identity.oraclecloud.com/oauth2/v1/token` |
| `ofjdbc.oauth.clientId`       | `OFJDBC_OAUTH_CLIENT_ID`      | connection `user`     | Client id |
| `ofjdbc.oauth.clientSecret`   | `OFJDBC_OAUTH_CLIENT_SECRET`  | connection `password` | Client secret |
| `ofjdbc.oauth.scope`          | `OFJDBC_OAUTH_SCOPE`          | —                    | Space-delimited scope of the Fusion resource |
| `ofjdbc.oauth.grantType`      | `OFJDBC_OAUTH_GRANT_TYPE`     | `client_credentials` | Grant type |
| `ofjdbc.oauth.clientAuth`     | `OFJDBC_OAUTH_CLIENT_AUTH`    | `post`               | `post` (form fields) or `basic` (HTTP Basic header) |
| `ofjdbc.oauth.extraParams`    | `OFJDBC_OAUTH_EXTRA_PARAMS`   | —                    | Extra form parameters, `audience=x&resource=y` |

**Example** (scheduled job):
```bash
export OFJDBC_OAUTH_TOKEN_ENDPOINT=https://idcs-<id>.identity.oraclecloud.com/oauth2/v1/token
export OFJDBC_OAUTH_CLIENT_ID=<client id>
export OFJDBC_OAUTH_CLIENT_SECRET=<client secret>
export OFJDBC_OAUTH_SCOPE="<scope of the Fusion application>"
```

The identity the token represents needs the same BI Publisher access as a user connecting with a password.
A rejected client fails the connection at once with the endpoint's `error: error_description`.

### Custom providers

To take credentials from a vault or use another flow, implement `my.jdbc.wsdl_driver.OAuthProvider`
(`getClientId`, `getClientSecret`, `getTokenEndpoint`, and optionally `getScope`, `getGrantType`,
`getClientAuthMethod`, `getAdditionalParameters`), put the class on the driver's classpath and name it with
`oauthProviderClass` (URL parameter or connection property; it implies `authType=OAUTH`; with `authType=OAUTH`
alone, `ofjdbc.oauth.providerClass` / `OFJDBC_OAUTH_PROVIDER_CLASS` are also read). The class needs a public
constructor `(String wsdlEndpoint, String user, String password)`, optionally followed by the connection
`Properties`, or a no-argument one.

In application containers the provider may be loaded by another class loader and implement a different copy of
the interface; the driver then accepts any object with the required methods and calls them reflectively, or
fails listing what is missing.


---

## DUCK Workspace (`OFJDBC_DUCK_DB`)

The `DUCK` schema is an embedded DuckDB database file. By default it is `~/.ofjdbc/duck.duckdb`, shared by all
Fusion connections, so data imported from different environments can be compared in one query.

Set it per connection with the JDBC URL parameter `?duckDb=<file>` or the connection property `ofjdbc.duckDb`,
or globally with `-Dofjdbc.duckDb` / `OFJDBC_DUCK_DB`. Use `:memory:` for a workspace without a file: nothing is
written to disk and its tables are gone once the last connection using it closes.

```bash
export OFJDBC_DUCK_DB=~/work/fusion-analysis.duckdb
```

**Transactions (`duckTransactions`, default `false`).** DUCK statements commit at once, whatever mode the client asks
for. With `?duckTransactions=true` Auto-commit off, Commit and Rollback apply to DUCK tables. `REFRESH` and imports are
all or nothing either way. Why this default: [DUCK Workspace Reference](duck_workspace.md#2-transactions).

**DuckDB version and old files.** The driver embeds DuckDB 2.0. Files written by DuckDB 1.x open as they are; a file
created by DuckDB 2.0 cannot be read by DuckDB 1.x. A file the driver cannot read is set aside with a warning instead
of failing the connection: [details](duck_workspace.md#12-duckdb-20-and-your-existing-files).

---

## Next result page cache (`OFJDBC_NEXT_PAGE_CACHE_SECONDS`)

DBeaver does not keep the cursor open between result pages: for the next page it closes the statement and runs
the same SQL again with a larger fetch size (100, 200, 300, …). The driver keeps the rows of the last query, including
the page prefetched while you looked at the previous one, and continues from them, so crossing a page boundary does
not read every row again from Fusion. Running the query again with the first page size (Refresh, a new run) or any
other query reads afresh.

The value is how long, in seconds since last use, the rows are kept: default `300`; `0` turns it off. The same setting
keeps the Fusion and REST parts of a mixed query for the next page.

---

## Query history (`OFJDBC_HISTORY`)

When on (`?history=true`; default off) the driver stores the rows of every Fusion query you run in the DUCK workspace,
so a slow result can be looked at again without Fusion: `HISTORY [LAST n]`, `HISTORY SHOW n`,
`HISTORY KEEP n AS DUCK.name [DECODE …]`, `HISTORY CLEAR`. The next result page of a query continues its entry instead
of starting a new one. `historyMaxRows` (default `1000000`) caps the rows kept in total; the oldest queries are dropped
first. An entry holds only the rows actually fetched — check `COMPLETE`. History needs the next result page cache
(not `0`) and writes into the shared workspace file, so enable it per connection when you want it.

---

## REST schema

`rest=false` hides the `REST` schema; `restApi` and `restVersion` choose the API (`latest` is the newest version
Fusion reports); `restMaxRows`, `restMaxParents`, `restMaxRequests` and `restMaxKeys` keep a statement from reading too
much (table [above](#connection-settings)). What each limit does and the messages you get: [REST Schema
Reference](rest_schema.md#7-settings).

---

## Query Profiler (`OFJDBC_PROFILE`)

With `?profile=true` in the JDBC URL (or connection property `ofjdbc.profile=true`, `-Dofjdbc.profile=true`,
`OFJDBC_PROFILE=true`) every fetched page adds an SQL warning to the statement, which DBeaver and
IntelliJ show in their Output panel:

```
[ofjdbc profile] page 2 | offset 2000 | 2000 rows (full, next page follows) | prefetched
  SOAP round trip: 4857 ms | body: 521 ms = network 456 ms + decode/parse 65 ms
  payload: base64 8.4 MB | wire 1.7 MB (gzip) | truncated: FALSE (</ROWSET> reached)
  SQL: select ... OFFSET 2000 ROWS FETCH NEXT 2000 ROWS ONLY
```

- **SOAP round trip**: request sent until response headers — BI Publisher executing the query plus latency.
- **body**: streaming the response while parsing it; split into waiting for the network and decoding/parsing.
- **truncated**: `FALSE` once the row set was read through `</ROWSET>`; a truncated page fails with an error
  instead of returning partial rows.
- **SQL**: exactly what was sent as `p_sql` (comments stripped, secured views applied, pagination added).

Off by default.

---

## Usage examples

A DBeaver connection: put connection settings in the URL or in Driver properties, for example
`jdbc:wsdl://<host>/xmlpserver/services/ExternalReportWSSService?WSDL:/Custom/Financials/RP_ARB.xdo?fetchSize=200&history=true`.

### Interactive work
```bash
export OFJDBC_DEFAULT_FETCH_SIZE=200
export OFJDBC_HTTP_CONNECT_TIMEOUT=20
export OFJDBC_HTTP_READ_TIMEOUT=120
```

### Bulk export or scheduled job
```bash
export OFJDBC_DEFAULT_FETCH_SIZE=5000
export OFJDBC_HTTP_READ_TIMEOUT=600
export OFJDBC_RETRY_MAX_ATTEMPTS=5
export OFJDBC_RETRY_BASE_DELAY_MS=5000
```

### JVM application
```bash
java -Dofjdbc.fetchSize=1000 -Dofjdbc.http.connectTimeout=20 -Dofjdbc.http.readTimeout=300 -jar your-application.jar
```

---

## Troubleshooting

**Slow queries** — raise `fetchSize` to reduce round trips; raise `OFJDBC_HTTP_READ_TIMEOUT` for queries that run
long in Fusion; `?profile=true` shows where the time goes.

**Memory** — lower `fetchSize`; watch the JVM heap with large pages.

**Timeouts** — raise `OFJDBC_HTTP_CONNECT_TIMEOUT` for a slow network, `OFJDBC_HTTP_READ_TIMEOUT` for slow queries;
more retries (`OFJDBC_RETRY_MAX_ATTEMPTS`) help with an unstable network.

**Fails fast after an outage** — the circuit breaker is open; it lets a trial request through after
`OFJDBC_CIRCUIT_BREAKER_TIMEOUT_MS` (default one minute).

**Values that are wrong** — a fetch size outside `1`–`10000` is capped with a log warning; HTTP and circuit breaker
values outside their range are moved to its nearest end; a prefetch threshold outside `0.0`–`1.0` is capped; a retry
value that is not a number is ignored. A non-numeric HTTP or circuit breaker value stops the driver from starting —
the log names it.

**Log messages to look for** — `Fetch size changed from X to Y (requested: Z)`,
`Fetch size X exceeds maximum Y, capping at maximum`, `Operation '…' succeeded on attempt N after Xms`.
