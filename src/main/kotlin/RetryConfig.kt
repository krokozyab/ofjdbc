package my.jdbc.wsdl_driver

import java.net.ConnectException
import java.net.http.HttpTimeoutException
import java.io.IOException
import java.sql.SQLException
import kotlin.random.Random

/**
 * Configuration for retry logic with exponential backoff
 */
data class RetryConfig(
    val maxAttempts: Int = 3,
    val baseDelayMs: Long = 1000L,
    val maxDelayMs: Long = 30000L,
    val multiplier: Double = 2.0,
    val jitterPercent: Double = 0.2
) {
    companion object {
        fun fromEnvironment(): RetryConfig {
            val maxAttempts = System.getenv("OFJDBC_RETRY_MAX_ATTEMPTS")?.toIntOrNull() 
                ?: System.getProperty("ofjdbc.retry.maxAttempts")?.toIntOrNull() 
                ?: 3
            
            val baseDelayMs = System.getenv("OFJDBC_RETRY_BASE_DELAY_MS")?.toLongOrNull()
                ?: System.getProperty("ofjdbc.retry.baseDelayMs")?.toLongOrNull()
                ?: 1000L
            
            val maxDelayMs = System.getenv("OFJDBC_RETRY_MAX_DELAY_MS")?.toLongOrNull()
                ?: System.getProperty("ofjdbc.retry.maxDelayMs")?.toLongOrNull()
                ?: 30000L
            
            val multiplier = System.getenv("OFJDBC_RETRY_MULTIPLIER")?.toDoubleOrNull()
                ?: System.getProperty("ofjdbc.retry.multiplier")?.toDoubleOrNull()
                ?: 2.0
            
            return RetryConfig(maxAttempts, baseDelayMs, maxDelayMs, multiplier)
        }
    }
    
    /**
     * Calculate delay for given attempt with exponential backoff and jitter
     */
    fun calculateDelay(attempt: Int): Long {
        val exponentialDelay = (baseDelayMs * Math.pow(multiplier, attempt.toDouble())).toLong()
        val cappedDelay = minOf(exponentialDelay, maxDelayMs)
        val jitterRange = (cappedDelay * jitterPercent).toLong()
        val jitter = Random.nextLong(-jitterRange, jitterRange + 1)
        return maxOf(0, cappedDelay + jitter)
    }
}

/**
 * Determines if an exception should trigger a retry
 */
fun isRetryableException(exception: Throwable): Boolean {
    return when (exception) {
        is HttpTimeoutException -> true
        is ConnectException -> true
        is IOException -> {
            val message = exception.message?.lowercase() ?: ""
            message.contains("connection reset") ||
            message.contains("connection refused") ||
            message.contains("network unreachable") ||
            message.contains("timeout")
        }
        is SQLException -> {
            val message = exception.message?.lowercase() ?: ""
            message.contains("connection") && (
                message.contains("timeout") ||
                message.contains("reset") ||
                message.contains("refused")
            )
        }
        else -> false
    }
}

/**
 * Determines if an HTTP status code should trigger a retry
 */
fun isRetryableHttpStatus(statusCode: Int): Boolean {
    return statusCode in setOf(500, 502, 503, 504, 408, 429)
}

/**
 * Context for tracking retry attempts
 */
data class RetryContext(
    val operation: String,
    val startTime: Long = System.currentTimeMillis(),
    var attempt: Int = 0,
    val exceptions: MutableList<Exception> = mutableListOf()
) {
    fun recordAttempt(exception: Exception?) {
        attempt++
        exception?.let { exceptions.add(it) }
    }
    
    fun totalElapsedMs(): Long = System.currentTimeMillis() - startTime
}