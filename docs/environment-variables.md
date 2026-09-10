# Environment Variables Configuration

This document describes all environment variables and system properties that can be used to configure the OFJDBC driver behavior.

## Table of Contents

- [HTTP Configuration](#http-configuration)
- [Fetch Size Configuration](#fetch-size-configuration)
- [Retry Configuration](#retry-configuration)
- [OAuth Authentication](#oauth-authentication)
- [System Properties](#system-properties)
- [Usage Examples](#usage-examples)

---

## HTTP Configuration

### `OFJDBC_HTTP_TIMEOUT_SECONDS`

**Description**: Sets the HTTP request timeout for WSDL service calls.

**Type**: Integer  
**Default**: `120` (2 minutes)  
**Range**: `1` - `3600` (1 second to 1 hour)  
**Used in**: `Utils.kt`

**Example**:
```bash
export OFJDBC_HTTP_TIMEOUT_SECONDS=180
```

**Impact**: Controls how long the driver waits for Oracle Fusion to respond to SQL queries. Increase for slow networks or complex queries.

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

## OAuth Authentication

OFJDBC can authenticate with an OAuth 2.0 Bearer token instead of Basic auth. Authentication is
provider-agnostic: you point the driver at a class implementing
`my.jdbc.wsdl_driver.OAuthProvider`, and the driver performs the token exchange itself (no extra
dependencies). New providers can be added without changing core driver logic.

### Enabling OAuth

Add the `oauthProviderClass` parameter to the JDBC URL (or as a connection property):

```
jdbc:wsdl://<host>/xmlpserver/services/ExternalReportWSSService?WSDL:/Custom/Financials/RP_ARB.xdo?oauthProviderClass=my.jdbc.wsdl_driver.SystemEnvOAuthProvider
```

The named class must implement `OAuthProvider` and have a public no-argument constructor; the driver
instantiates it reflectively, requests an access token, and sends it as `Authorization: Bearer <token>`
on every request. Tokens are cached and refreshed automatically before expiry. When
`oauthProviderClass` is absent, the driver behaves exactly as before (Basic auth), preserving backward
compatibility.

### Reference provider: `SystemEnvOAuthProvider`

Reads all OAuth configuration from environment variables (each also supports the dotted lowercase
system-property fallback, e.g. `ofjdbc.oauth.client.id`):

| Variable | Required | Default | Description |
|----------|----------|---------|-------------|
| `OFJDBC_OAUTH_CLIENT_ID` | Yes | — | OAuth client identifier |
| `OFJDBC_OAUTH_CLIENT_SECRET` | No | `""` | OAuth client secret |
| `OFJDBC_OAUTH_TOKEN_ENDPOINT` | Yes | — | Token endpoint URL |
| `OFJDBC_OAUTH_SCOPE` | No | — | Space-delimited scope |
| `OFJDBC_OAUTH_GRANT_TYPE` | No | `client_credentials` | OAuth grant type |
| `OFJDBC_OAUTH_CLIENT_AUTH` | No | `post` | `post` (body fields) or `basic` (HTTP Basic header) |
| `OFJDBC_OAUTH_EXTRA_PARAMS` | No | — | Extra form params, e.g. `audience=x&resource=y` |

**Example**:
```bash
export OFJDBC_OAUTH_CLIENT_ID=my-client
export OFJDBC_OAUTH_CLIENT_SECRET=my-secret
export OFJDBC_OAUTH_TOKEN_ENDPOINT=https://idp.example.com/oauth2/token
export OFJDBC_OAUTH_SCOPE="fusion.read"
```

### Custom providers

Implement `my.jdbc.wsdl_driver.OAuthProvider` (client id/secret, token endpoint, optional scope,
grant type, client-auth method, and additional parameters), package it on the driver classpath, and
reference it via `oauthProviderClass`. No core driver changes are required.

---

## System Properties

All environment variables can also be set as JVM system properties with the `ofjdbc.` prefix:

| Environment Variable          | System Property            |
|-------------------------------|----------------------------|
| `OFJDBC_HTTP_TIMEOUT_SECONDS` | `ofjdbc.httpTimeout`       |
| `OFJDBC_DEFAULT_FETCH_SIZE`   | `ofjdbc.fetchSize`         |
| `OFJDBC_RETRY_MAX_ATTEMPTS`   | `ofjdbc.retry.maxAttempts` |
| `OFJDBC_RETRY_BASE_DELAY_MS`  | `ofjdbc.retry.baseDelayMs` |
| `OFJDBC_RETRY_MAX_DELAY_MS`   | `ofjdbc.retry.maxDelayMs`  |
| `OFJDBC_RETRY_MULTIPLIER`     | `ofjdbc.retry.multiplier`  |

**Example**:
```bash
java -Dofjdbc.fetchSize=1000 -Dofjdbc.httpTimeout=180 -jar your-application.jar
```

---

## Usage Examples

### Development Environment
```bash
# Fast response for development
export OFJDBC_DEFAULT_FETCH_SIZE=100
export OFJDBC_HTTP_TIMEOUT_SECONDS=60
export OFJDBC_RETRY_MAX_ATTEMPTS=2
```

### Production Environment
```bash
# Optimized for reliability and performance
export OFJDBC_DEFAULT_FETCH_SIZE=1000
export OFJDBC_HTTP_TIMEOUT_SECONDS=300
export OFJDBC_RETRY_MAX_ATTEMPTS=5
export OFJDBC_RETRY_BASE_DELAY_MS=2000
export OFJDBC_RETRY_MAX_DELAY_MS=60000
```

### DBeaver Optimization
```bash
# Optimized for interactive database browsing
export OFJDBC_DEFAULT_FETCH_SIZE=200
export OFJDBC_HTTP_TIMEOUT_SECONDS=120
export OFJDBC_RETRY_MAX_ATTEMPTS=3
```

### Bulk Data Export
```bash
# Optimized for large data exports
export OFJDBC_DEFAULT_FETCH_SIZE=5000
export OFJDBC_HTTP_TIMEOUT_SECONDS=600
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
- Increase `OFJDBC_HTTP_TIMEOUT_SECONDS` for complex queries

**Memory Issues**:
- Decrease `OFJDBC_DEFAULT_FETCH_SIZE` to reduce memory usage
- Monitor JVM heap size with large fetch sizes

**Connection Timeouts**:
- Increase `OFJDBC_HTTP_TIMEOUT_SECONDS`
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

---

