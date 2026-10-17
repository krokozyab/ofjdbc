# Environment Variables Configuration

This document describes all environment variables and system properties that can be used to configure the OFJDBC driver behavior.

## Table of Contents

- [HTTP Configuration](#http-configuration)
- [Fetch Size Configuration](#fetch-size-configuration)
- [Retry Configuration](#retry-configuration)
- [System Properties](#system-properties)
- [OAuth Authentication](#oauth-authentication)
- [DUCK Workspace](#duck-workspace-ofjdbc_duck_db)
- [Usage Examples](#usage-examples)
- [Query Profiler](#query-profiler-ofjdbc_profile)

---

## HTTP Configuration

### `OFJDBC_HTTP_CONNECT_TIMEOUT`

**Description**: Sets the HTTP connection timeout (time to establish TCP connection).

**Type**: Integer  
**Default**: `30` seconds  
**Range**: `1` - `300` seconds  
**System Property**: `ofjdbc.http.connectTimeout`  
**Used in**: `HttpClient.kt` (`HttpConfig.CONNECT_TIMEOUT_SECONDS`)

**Example**:
```bash
export OFJDBC_HTTP_CONNECT_TIMEOUT=20
```

**Impact**: Lower values fail fast when Oracle Fusion is unreachable. Higher values tolerate slower network setup.

### `OFJDBC_HTTP_READ_TIMEOUT`

**Description**: Sets the HTTP read timeout (time waiting for response data after connection is established).

**Type**: Integer  
**Default**: `120` seconds  
**Range**: `5` - `1800` seconds  
**System Property**: `ofjdbc.http.readTimeout`  
**Used in**: `HttpClient.kt` (`HttpConfig.READ_TIMEOUT_SECONDS`)

**Example**:
```bash
export OFJDBC_HTTP_READ_TIMEOUT=300
```

**Impact**: Increase for complex queries or large result sets. Lower values fail faster on slow responses.

---

## Fetch Size Configuration

### `OFJDBC_DEFAULT_FETCH_SIZE`

**Description**: Sets the default number of rows fetched per page from Oracle Fusion.

**Type**: Integer  
**Default**: `500`  
**Range**: `1` - `10000`  
**Used in**: `WsdlStatement.kt`

**Example**:
```bash
export OFJDBC_DEFAULT_FETCH_SIZE=1000
```

Can also be set per connection via the JDBC URL parameter `?fetchSize=1000` or the
connection property `ofjdbc.fetchSize` (e.g. in DBeaver's driver properties dialog);
those take precedence over the system property and this environment variable.

**Impact**: 
- **Smaller values** (50-200): Faster initial response, good for interactive browsing
- **Larger values** (1000+): Better performance for bulk operations, fewer round trips

### `OFJDBC_MAX_FETCH_SIZE`

**Description**: Maximum allowed fetch size to prevent memory issues.

**Type**: Integer  
**Default**: `10000`  
**Range**: `100` - `50000`  
**Used in**: `WsdlStatement.kt` (hardcoded constant, configurable in future versions)

**Impact**: Prevents users from setting extremely large fetch sizes that could cause memory problems.

---

## Retry Configuration

### `OFJDBC_RETRY_MAX_ATTEMPTS`

**Description**: Maximum number of retry attempts for failed operations.

**Type**: Integer  
**Default**: `3`  
**Range**: `1` - `10`  
**Used in**: `RetryConfig.kt`

**Example**:
```bash
export OFJDBC_RETRY_MAX_ATTEMPTS=5
```

**Impact**: Higher values increase resilience but may cause longer delays on permanent failures.

### `OFJDBC_RETRY_BASE_DELAY_MS`

**Description**: Initial delay in milliseconds before first retry attempt.

**Type**: Long  
**Default**: `1000` (1 second)  
**Range**: `100` - `30000` (100ms to 30 seconds)  
**Used in**: `RetryConfig.kt`

**Example**:
```bash
export OFJDBC_RETRY_BASE_DELAY_MS=2000
```

**Impact**: Controls the starting point for exponential backoff. Higher values reduce server load but increase response time.

### `OFJDBC_RETRY_MAX_DELAY_MS`

**Description**: Maximum delay in milliseconds between retry attempts.

**Type**: Long  
**Default**: `30000` (30 seconds)  
**Range**: `1000` - `300000` (1 second to 5 minutes)  
**Used in**: `RetryConfig.kt`

**Example**:
```bash
export OFJDBC_RETRY_MAX_DELAY_MS=60000
```

**Impact**: Caps the exponential backoff to prevent extremely long waits.

### `OFJDBC_RETRY_MULTIPLIER`

**Description**: Multiplier for exponential backoff between retry attempts.

**Type**: Double  
**Default**: `2.0`  
**Range**: `1.1` - `5.0`  
**Used in**: `RetryConfig.kt`

**Example**:
```bash
export OFJDBC_RETRY_MULTIPLIER=1.5
```

**Impact**: Controls how quickly retry delays increase. Lower values = more gradual increase.

---

## System Properties

All environment variables can also be set as JVM system properties with the `ofjdbc.` prefix:

| Environment Variable          | System Property            |
|-------------------------------|----------------------------|
| `OFJDBC_HTTP_CONNECT_TIMEOUT` | `ofjdbc.http.connectTimeout` |
| `OFJDBC_HTTP_READ_TIMEOUT`    | `ofjdbc.http.readTimeout`    |
| `OFJDBC_DEFAULT_FETCH_SIZE`   | `ofjdbc.fetchSize`         |
| `OFJDBC_RETRY_MAX_ATTEMPTS`   | `ofjdbc.retry.maxAttempts` |
| `OFJDBC_RETRY_BASE_DELAY_MS`  | `ofjdbc.retry.baseDelayMs` |
| `OFJDBC_RETRY_MAX_DELAY_MS`   | `ofjdbc.retry.maxDelayMs`  |
| `OFJDBC_RETRY_MULTIPLIER`     | `ofjdbc.retry.multiplier`  |
| `OFJDBC_PREFETCH_ENABLED`     | `ofjdbc.prefetch.enabled`  |
| `OFJDBC_PREFETCH_THRESHOLD`   | `ofjdbc.prefetch.threshold` |
| `OFJDBC_METADATA_PREFETCH_ENABLED` | `ofjdbc.metadata.prefetchEnabled` |
| `OFJDBC_PROFILE`              | `ofjdbc.profile`           |
| `OFJDBC_DUCK_DB`              | `ofjdbc.duckDb`            |
| `OFJDBC_OAUTH_*`              | `ofjdbc.oauth.*` (see [OAuth](#oauth-authentication)) |

Resolution priority for every setting: JDBC URL parameter > connection Properties > system property > environment variable > default.

**Example**:
```bash
java -Dofjdbc.fetchSize=1000 -Dofjdbc.http.connectTimeout=20 -Dofjdbc.http.readTimeout=300 -jar your-application.jar
```

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

**Example**:
```bash
export OFJDBC_DUCK_DB=~/work/fusion-analysis.duckdb
```

---

## Usage Examples

### Development Environment
```bash
# Fast response for development
export OFJDBC_DEFAULT_FETCH_SIZE=100
export OFJDBC_HTTP_CONNECT_TIMEOUT=15
export OFJDBC_HTTP_READ_TIMEOUT=60
export OFJDBC_RETRY_MAX_ATTEMPTS=2
```

### Production Environment
```bash
# Optimized for reliability and performance
export OFJDBC_DEFAULT_FETCH_SIZE=1000
export OFJDBC_HTTP_CONNECT_TIMEOUT=30
export OFJDBC_HTTP_READ_TIMEOUT=300
export OFJDBC_RETRY_MAX_ATTEMPTS=5
export OFJDBC_RETRY_BASE_DELAY_MS=2000
export OFJDBC_RETRY_MAX_DELAY_MS=60000
```

### DBeaver Optimization
```bash
# Optimized for interactive database browsing
export OFJDBC_DEFAULT_FETCH_SIZE=200
export OFJDBC_HTTP_CONNECT_TIMEOUT=20
export OFJDBC_HTTP_READ_TIMEOUT=120
export OFJDBC_RETRY_MAX_ATTEMPTS=3
```

### Bulk Data Export
```bash
# Optimized for large data exports
export OFJDBC_DEFAULT_FETCH_SIZE=5000
export OFJDBC_HTTP_CONNECT_TIMEOUT=30
export OFJDBC_HTTP_READ_TIMEOUT=600
export OFJDBC_RETRY_MAX_ATTEMPTS=5
export OFJDBC_RETRY_BASE_DELAY_MS=5000
```

---

## Configuration Priority

The driver uses the following priority order for configuration:

1. **JDBC URL parameters** (highest priority)
2. **System properties** (`-Dofjdbc.property=value`)
3. **Environment variables** (`OFJDBC_VARIABLE=value`)
4. **Default values** (lowest priority)

---

## Troubleshooting

### Common Issues

**Slow Query Performance**:
- Increase `OFJDBC_DEFAULT_FETCH_SIZE` to reduce round trips
- Increase `OFJDBC_HTTP_READ_TIMEOUT` for complex queries

**Memory Issues**:
- Decrease `OFJDBC_DEFAULT_FETCH_SIZE` to reduce memory usage
- Monitor JVM heap size with large fetch sizes

**Connection Timeouts**:
- Increase `OFJDBC_HTTP_CONNECT_TIMEOUT` if network/TCP setup is slow
- Increase `OFJDBC_HTTP_READ_TIMEOUT` if query execution is slow
- Increase `OFJDBC_RETRY_MAX_ATTEMPTS`
- Adjust `OFJDBC_RETRY_BASE_DELAY_MS` for network conditions

**Too Many Retries**:
- Decrease `OFJDBC_RETRY_MAX_ATTEMPTS`
- Increase `OFJDBC_RETRY_BASE_DELAY_MS` to reduce server load

### Validation

The driver validates all configuration values and will:
- **Log warnings** for values outside recommended ranges
- **Cap values** at maximum limits to prevent issues
- **Use defaults** for invalid or missing values
- **Throw exceptions** for critically invalid values (e.g., negative timeouts)

---

## Monitoring

Look for log messages like:
- `Fetch size changed from X to Y (requested: Z)`
- `Operation 'WSDL SQL Execution' succeeded on attempt N after Xms`
- `Fetch size X exceeds maximum Y, capping at maximum`

### Query profiler (`OFJDBC_PROFILE`)

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
