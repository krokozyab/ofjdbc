# OFJDBC Development Guidelines

This document provides essential information for developers working on the OFJDBC (Oracle Fusion JDBC) driver project.

## Project Overview

OFJDBC is a read-only JDBC driver that enables SQL queries against Oracle Fusion via WSDL. It serves as a bridge between standard SQL tools/IDEs and Oracle Fusion's reporting services, allowing developers and analysts to use familiar SQL interfaces to access Oracle Fusion data.

## Build and Configuration

### Build Requirements

- JDK 11 or higher
- Kotlin 2.2.20-Beta2 or compatible version
- Gradle 8.8 or higher

### Building the Project

The project uses Gradle with the Kotlin DSL for build configuration. Key plugins include:

- `kotlin("jvm")`: For Kotlin JVM compilation
- `com.github.johnrengelman.shadow`: For creating a fat JAR with all dependencies
- `com.github.ben-manes.versions`: For dependency version checking

To build the project:

```bash
# Build the project
./gradlew build

# Create a fat JAR with all dependencies
./gradlew shadowJar
```

The resulting JAR will be in the `build/libs` directory.

### JVM Toolchain

The project is configured to use JVM toolchain 11:

```kotlin
kotlin {
    jvmToolchain(11)
}
```

Ensure your development environment has JDK 11 or higher available.

## Environment Variables and Configuration

The driver supports configuration through environment variables and system properties. All environment variables have equivalent system properties with the `ofjdbc.` prefix.

### HTTP Configuration

- `OFJDBC_HTTP_TIMEOUT_SECONDS` / `ofjdbc.httpTimeout`: Sets the HTTP request timeout for WSDL service calls
  - Default: `120` (2 minutes)
  - Range: `1` - `3600` (1 second to 1 hour)

### Fetch Size Configuration

- `OFJDBC_DEFAULT_FETCH_SIZE` / `ofjdbc.fetchSize`: Sets the default number of rows fetched per page
  - Default: `500`
  - Range: `1` - `10000`
  - Smaller values (50-200): Faster initial response, good for interactive browsing
  - Larger values (1000+): Better performance for bulk operations, fewer round trips

- `OFJDBC_MAX_FETCH_SIZE`: Maximum allowed fetch size to prevent memory issues
  - Default: `10000`
  - Range: `100` - `50000`

### Retry Configuration

- `OFJDBC_RETRY_MAX_ATTEMPTS` / `ofjdbc.retry.maxAttempts`: Maximum number of retry attempts for failed operations
  - Default: `3`
  - Range: `1` - `10`

- `OFJDBC_RETRY_BASE_DELAY_MS` / `ofjdbc.retry.baseDelayMs`: Initial delay in milliseconds before first retry attempt
  - Default: `1000` (1 second)
  - Range: `100` - `30000` (100ms to 30 seconds)

- `OFJDBC_RETRY_MAX_DELAY_MS` / `ofjdbc.retry.maxDelayMs`: Maximum delay in milliseconds between retry attempts
  - Default: `30000` (30 seconds)
  - Range: `1000` - `300000` (1 second to 5 minutes)

- `OFJDBC_RETRY_MULTIPLIER` / `ofjdbc.retry.multiplier`: Multiplier for exponential backoff between retry attempts
  - Default: `2.0`
  - Range: `1.1` - `5.0`

### Configuration Priority

The driver uses the following priority order for configuration:

1. JDBC URL parameters (highest priority)
2. System properties (`-Dofjdbc.property=value`)
3. Environment variables (`OFJDBC_VARIABLE=value`)
4. Default values (lowest priority)

## Code Structure

### Key Components

- **WsdlDriver**: Entry point for the JDBC driver, handles connection creation
- **WsdlConnection**: Implements the JDBC Connection interface
- **WsdlStatement**: Executes SQL queries via WSDL
- **WsdlDatabaseMetaData**: Provides metadata about the database
- **Utils**: Contains utility functions for HTTP requests, XML processing, and SOAP communication
- **RetryConfig**: Configures the retry mechanism for failed operations
- **SecuredViewMappings**: Maps table names to their secured view equivalents
- **XmlResultSet**: Implements the JDBC ResultSet interface for XML data
- **PaginatedResultSet**: Handles pagination of large result sets

### Package Structure

The project uses a flat package structure with all classes in the `my.jdbc.wsdl_driver` package.

## Development Guidelines

### Code Style

- **Kotlin Idioms**: Use Kotlin idioms and features where appropriate
- **Error Handling**: Use exceptions for error conditions, with descriptive messages
- **Logging**: Use SLF4J for logging, with appropriate log levels
- **Documentation**: Document public APIs with KDoc comments
- **Immutability**: Prefer immutable data structures and properties where possible

### Error Handling and Retry Mechanism

The project implements a robust error handling and retry mechanism:

1. **Retry Configuration**: Configurable via environment variables or system properties
2. **Exponential Backoff**: Increases delay between retries to avoid overwhelming the server
3. **Jitter**: Adds random variation to retry delays to prevent synchronized retries
4. **Retryable Exceptions**: Only specific exceptions trigger retries (network issues, timeouts)
5. **Retryable HTTP Status Codes**: Only specific HTTP status codes trigger retries (500, 502, 503, 504, 408, 429)

When implementing new features that involve network communication, use the `executeWithRetry` function:

```kotlin
executeWithRetry("Operation Name") {
    // Code that might fail and should be retried
}
```

### XML Processing

The project includes robust XML processing with fallback mechanisms for malformed XML:

1. **Initial Parse**: Attempts to parse XML as-is
2. **Cleaning**: If initial parse fails, applies cleaning steps:
   - Escapes stray '&' characters
   - Removes leading BOM/whitespace
   - Collapses illegal spaces
   - Wraps elements with CDATA if needed
3. **Fallback**: If cleaning doesn't work, applies more aggressive sanitization

When working with XML responses, use the `parseXml` function to handle these edge cases.

### Secured Views Mapping

Oracle HR tables with sensitive information must be accessed through secured views. The driver automatically substitutes table names with their secured equivalents before sending SQL to the WSDL service.

The mapping is defined in `SecuredViewMappings.kt`. When adding new secured views:

1. Add the mapping to the `tableToView` map
2. Ensure the regex pattern in the `apply` function correctly identifies the table name

## Testing

The project doesn't currently have automated tests. When implementing tests:

1. Use JUnit 5 for test framework
2. Mock external dependencies (HTTP client, XML parser)
3. Test edge cases, especially for XML parsing and error handling
4. Consider adding integration tests with a mock WSDL server

## Performance Considerations

- **Connection Pooling**: The driver doesn't implement connection pooling; use a connection pool manager if needed
- **Fetch Size**: Adjust fetch size based on the use case (smaller for interactive, larger for bulk)
- **HTTP Client**: The driver reuses a single HttpClient instance for all connections
- **XML Processing**: XML processing can be CPU-intensive; consider caching results where appropriate

## Deployment

The project uses the Shadow plugin to create a fat JAR with all dependencies. This JAR can be deployed directly to client applications or used with database tools like DBeaver or DbVisualizer.

## Troubleshooting

Common issues and solutions:

- **Slow Query Performance**: Increase `OFJDBC_DEFAULT_FETCH_SIZE` to reduce round trips
- **Memory Issues**: Decrease `OFJDBC_DEFAULT_FETCH_SIZE` to reduce memory usage
- **Connection Timeouts**: Increase `OFJDBC_HTTP_TIMEOUT_SECONDS` and `OFJDBC_RETRY_MAX_ATTEMPTS`
- **XML Parsing Errors**: Check for malformed XML in the response; the driver attempts to handle this but may fail with extremely malformed XML