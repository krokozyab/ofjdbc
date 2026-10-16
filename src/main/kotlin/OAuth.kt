package my.jdbc.wsdl_driver

import org.slf4j.LoggerFactory
import java.net.URI
import java.net.URLEncoder
import java.net.http.HttpRequest
import java.net.http.HttpResponse
import java.nio.charset.StandardCharsets
import java.time.Duration
import java.util.Base64
import java.util.concurrent.ConcurrentHashMap

private val oauthLogger = LoggerFactory.getLogger("OAuth")

/**
 * Generic, provider-agnostic contract that supplies the configuration required to
 * obtain an OAuth 2.0 access token. Implementations only describe *where* and *how*
 * to authenticate; the actual token acquisition (HTTP exchange) is performed by the
 * driver itself in [acquireOAuthToken]. This keeps the core driver logic unchanged
 * when new providers are added — a new provider only implements this interface.
 *
 * Implementations MUST be instantiable via a constructor accepting (String, String, String)
 * so the driver can create them reflectively from the `oauthProviderClass` JDBC URL parameter.
 * The constructor parameters are: wsdlEndpoint, user, and pass .
 */
interface OAuthProvider {

    /** OAuth client identifier. */
    fun getClientId(): String

    /** OAuth client secret. May be blank for public clients. */
    fun getClientSecret(): String

    /** Absolute URL of the token endpoint used to request an access token. */
    fun getTokenEndpoint(): String

    /** Optional space-delimited scope string, or `null`/blank when not required. */
    fun getScope(): String? = null

    /**
     * OAuth grant type. Defaults to the client-credentials flow, which is the common
     * machine-to-machine flow for a JDBC driver.
     */
    fun getGrantType(): String = "client_credentials"

    /**
     * How the client credentials are transmitted to the token endpoint:
     *  - "basic": sent as an HTTP Basic `Authorization` header (RFC 6749 §2.3.1).
     *  - "post" : sent as `client_id`/`client_secret` form fields in the request body.
     */
    fun getClientAuthMethod(): String = "post"

    /**
     * Any additional form parameters required by a specific OAuth flow
     * (e.g. `audience`, `resource`, `assertion`). Sent as `application/x-www-form-urlencoded`
     * body fields alongside the standard parameters.
     */
    fun getAdditionalParameters(): Map<String, String> = emptyMap()
}

/**
 * Reference [OAuthProvider] implementation that reads every configuration value from
 * system environment variables or JVM flags. Useful for headless / CI usage and as a template for
 * building custom providers.
 *
 * Environment variables:
 *  - `OFJDBC_OAUTH_CLIENT_ID`       (optional, username passed to constructor is used if not set)
 *  - `OFJDBC_OAUTH_CLIENT_SECRET`   (optional, password passed to constructor is used if not set)
 *  - `OFJDBC_OAUTH_TOKEN_ENDPOINT`  (required)
 *  - `OFJDBC_OAUTH_SCOPE`           (optional)
 *  - `OFJDBC_OAUTH_GRANT_TYPE`      (optional, default "client_credentials")
 *  - `OFJDBC_OAUTH_CLIENT_AUTH`     (optional, default "post"; "basic" to use HTTP Basic)
 *  - `OFJDBC_OAUTH_EXTRA_PARAMS`    (optional, `k1=v1&k2=v2` extra form parameters)
 */
class SystemEnvOAuthProvider(val wsdlEndpoint: String, val user: String?, val pass: String?) : OAuthProvider {

    private fun env(name: String): String? =
        System.getenv(name) ?: System.getProperty(name.lowercase().replace('_', '.'))

    override fun getClientId(): String =

        env("OFJDBC_OAUTH_CLIENT_ID")
            ?: user
            ?: throw IllegalStateException("OFJDBC_OAUTH_CLIENT_ID is not set")

    override fun getClientSecret(): String = env("OFJDBC_OAUTH_CLIENT_SECRET") ?: pass ?:  throw IllegalStateException("OFJDBC_OAUTH_CLIENT_SECRET is not set")

    override fun getTokenEndpoint(): String =
        env("OFJDBC_OAUTH_TOKEN_ENDPOINT")
            ?: throw IllegalStateException("OFJDBC_OAUTH_TOKEN_ENDPOINT is not set")

    override fun getScope(): String? = env("OFJDBC_OAUTH_SCOPE")?.takeIf { it.isNotBlank() }

    override fun getGrantType(): String =
        env("OFJDBC_OAUTH_GRANT_TYPE")?.takeIf { it.isNotBlank() } ?: "client_credentials"

    override fun getClientAuthMethod(): String =
        env("OFJDBC_OAUTH_CLIENT_AUTH")?.takeIf { it.isNotBlank() } ?: "post"

    override fun getAdditionalParameters(): Map<String, String> {
        val raw = env("OFJDBC_OAUTH_EXTRA_PARAMS")?.takeIf { it.isNotBlank() } ?: return emptyMap()
        return raw.split("&").mapNotNull { pair ->
            val idx = pair.indexOf('=')
            if (idx <= 0) null else pair.substring(0, idx).trim() to pair.substring(idx + 1).trim()
        }.toMap()
    }
}

/**
 * Adapts an arbitrary object that *structurally* matches [OAuthProvider] (i.e. exposes the same
 * public methods) but is not assignable to it, which happens in application containers using
 * hierarchical class loading where the driver and the provider class see two distinct
 * `my.jdbc.wsdl_driver.OAuthProvider` types loaded by different class loaders.
 *
 * All calls are delegated reflectively to [delegate]. Use [OAuthProviderWrapper.wrapIfCompatible]
 * to obtain an instance after validating the delegate's methods.
 */
class OAuthProviderWrapper private constructor(
    private val delegate: Any,
    private val methods: Map<String, java.lang.reflect.Method>
) : OAuthProvider {

    private fun invoke(name: String): Any? {
        val method = methods[name] ?: throw IllegalStateException(
            "Method '$name' is not available on ${delegate.javaClass.name}"
        )
        return try {
            method.invoke(delegate)
        } catch (e: java.lang.reflect.InvocationTargetException) {
            throw e.targetException ?: e
        }
    }

    private fun invokeString(name: String): String? = invoke(name) as String?

    override fun getClientId(): String =
        invokeString("getClientId") ?: throw IllegalStateException("getClientId() returned null")

    override fun getClientSecret(): String =
        invokeString("getClientSecret") ?: throw IllegalStateException("getClientSecret() returned null")

    override fun getTokenEndpoint(): String =
        invokeString("getTokenEndpoint") ?: throw IllegalStateException("getTokenEndpoint() returned null")

    override fun getScope(): String? =
        if (methods.containsKey("getScope")) invokeString("getScope") else null

    override fun getGrantType(): String =
        if (methods.containsKey("getGrantType")) invokeString("getGrantType") ?: "client_credentials"
        else "client_credentials"

    override fun getClientAuthMethod(): String =
        if (methods.containsKey("getClientAuthMethod")) invokeString("getClientAuthMethod") ?: "post"
        else "post"

    @Suppress("UNCHECKED_CAST")
    override fun getAdditionalParameters(): Map<String, String> =
        if (methods.containsKey("getAdditionalParameters"))
            (invoke("getAdditionalParameters") as Map<String, String>?) ?: emptyMap()
        else emptyMap()

    companion object {

        /** Methods an object must expose to be usable as an [OAuthProvider]. */
        private val REQUIRED = mapOf(
            "getClientId" to String::class.java,
            "getClientSecret" to String::class.java,
            "getTokenEndpoint" to String::class.java
        )

        /** Methods that may be absent, in which case the interface defaults are used. */
        private val OPTIONAL = mapOf(
            "getScope" to String::class.java,
            "getGrantType" to String::class.java,
            "getClientAuthMethod" to String::class.java,
            "getAdditionalParameters" to Map::class.java
        )

        /**
         * Returns the candidate wrapped as an [OAuthProvider] when it structurally matches the
         * interface, or `null` when one or more required methods are missing or have an
         * incompatible signature. [missing] receives a human-readable description of every
         * mismatch so callers can produce a helpful error message.
         */
        fun wrapIfCompatible(candidate: Any, missing: MutableList<String> = mutableListOf()): OAuthProviderWrapper? {
            val resolved = LinkedHashMap<String, java.lang.reflect.Method>()

            for ((name, returnType) in REQUIRED) {
                val method = findNoArgMethod(candidate.javaClass, name)
                when {
                    method == null -> missing += "missing method '$name()'"
                    !returnType.isAssignableFrom(method.returnType) ->
                        missing += "method '$name()' returns ${method.returnType.name}, expected ${returnType.name}"
                    else -> resolved[name] = method
                }
            }

            for ((name, returnType) in OPTIONAL) {
                val method = findNoArgMethod(candidate.javaClass, name) ?: continue
                if (returnType.isAssignableFrom(method.returnType)) resolved[name] = method
            }

            if (missing.isNotEmpty()) return null
            return OAuthProviderWrapper(candidate, resolved)
        }

        /**
         * Finds a public, non-static, zero-argument method named [name] on [type] or any of its
         * supertypes and makes it accessible, so providers declared in non-exported/non-public
         * classes still work.
         */
        private fun findNoArgMethod(type: Class<*>, name: String): java.lang.reflect.Method? {
            val method = type.methods.firstOrNull {
                it.name == name &&
                    it.parameterCount == 0 &&
                    !java.lang.reflect.Modifier.isStatic(it.modifiers)
            } ?: return null
            return try {
                method.isAccessible = true
                method
            } catch (_: RuntimeException) {
                method
            }
        }
    }
}

/** An OAuth access token together with the epoch millis at which it should be considered expired. */
data class OAuthToken(val accessToken: String, val expiresAtEpochMs: Long) {
    /** Treat the token as expired slightly early to avoid using it right at the boundary. */
    fun isExpired(skewMs: Long = 30_000L): Boolean =
        System.currentTimeMillis() >= (expiresAtEpochMs - skewMs)
}

/**
 * Performs the OAuth token exchange described by [provider] and returns the resulting token.
 * Uses only the JDK HTTP client and a small regex-based JSON reader (no extra dependencies).
 */
fun acquireOAuthToken(provider: OAuthProvider): OAuthToken {
    val form = LinkedHashMap<String, String>()
    form["grant_type"] = provider.getGrantType()
    provider.getScope()?.takeIf { it.isNotBlank() }?.let { form["scope"] = it }
    form.putAll(provider.getAdditionalParameters())

    val useBasic = provider.getClientAuthMethod().equals("basic", ignoreCase = true)
    if (!useBasic) {
        form["client_id"] = provider.getClientId()
        if (provider.getClientSecret().isNotEmpty()) form["client_secret"] = provider.getClientSecret()
    }

    val body = form.entries.joinToString("&") { (k, v) ->
        URLEncoder.encode(k, StandardCharsets.UTF_8) + "=" + URLEncoder.encode(v, StandardCharsets.UTF_8)
    }

    val requestBuilder = HttpRequest.newBuilder()
        .uri(URI.create(provider.getTokenEndpoint()))
        .header("Content-Type", "application/x-www-form-urlencoded")
        .header("Accept", "application/json")
        .timeout(Duration.ofSeconds(60))
        .POST(HttpRequest.BodyPublishers.ofString(body))

    if (useBasic) {
        val raw = "${provider.getClientId()}:${provider.getClientSecret()}"
        val encoded = Base64.getEncoder().encodeToString(raw.toByteArray(StandardCharsets.UTF_8))
        requestBuilder.header("Authorization", "Basic $encoded")
    }

    oauthLogger.trace("Requesting OAuth token from {}", provider.getTokenEndpoint())
    val response = executeWithRetry("OAuth token request") {
        val resp = HttpClientManager.httpClient.send(requestBuilder.build(), HttpResponse.BodyHandlers.ofString())
        if (isRetryableHttpStatus(resp.statusCode())) {
            throw java.sql.SQLException("OAuth token endpoint returned retryable error (${resp.statusCode()}): ${resp.body()}")
        }
        resp
    }

    val status = response.statusCode()
    val respBody = response.body() ?: ""
    if (status !in 200..299) {
        throw java.sql.SQLException("OAuth token request failed (HTTP $status): $respBody")
    }

    val accessToken = extractJsonString(respBody, "access_token")
        ?: throw java.sql.SQLException("OAuth token response did not contain an access_token: $respBody")
    val expiresInSec = extractJsonNumber(respBody, "expires_in") ?: 3600L
    val expiresAt = System.currentTimeMillis() + expiresInSec * 1000L

    oauthLogger.trace("Obtained OAuth access token (expires in {}s)", expiresInSec)
    return OAuthToken(accessToken, expiresAt)
}

/** Minimal JSON string-field reader for flat token responses (no JSON dependency available). */
private fun extractJsonString(json: String, field: String): String? {
    val regex = Regex("\"" + Regex.escape(field) + "\"\\s*:\\s*\"([^\"]*)\"")
    return regex.find(json)?.groupValues?.get(1)
}

/** Minimal JSON numeric-field reader for flat token responses. */
private fun extractJsonNumber(json: String, field: String): Long? {
    val regex = Regex("\"" + Regex.escape(field) + "\"\\s*:\\s*\"?(\\d+)\"?")
    return regex.find(json)?.groupValues?.get(1)?.toLongOrNull()
}

/**
 * Strategy that produces the value of the HTTP `Authorization` header for outgoing WSDL requests.
 * Introducing this abstraction lets the driver support Basic authentication and OAuth (Bearer)
 * without changing the request-sending code paths.
 */
interface Authenticator {
    fun authorizationHeader(): String
}

/** Classic HTTP Basic authentication using a username and password. */
class BasicAuthenticator(private val username: String, private val password: String) : Authenticator {
    override fun authorizationHeader(): String = encodeCredentials(username, password)
}

/**
 * OAuth Bearer authentication. Lazily acquires an access token via [acquireOAuthToken] and caches
 * it until shortly before expiry, transparently refreshing when needed. Thread-safe.
 */
class OAuthAuthenticator(private val provider: OAuthProvider) : Authenticator {
    @Volatile
    private var cached: OAuthToken? = null
    private val lock = Any()

    private fun currentToken(): OAuthToken {
        val existing = cached
        if (existing != null && !existing.isExpired()) return existing
        synchronized(lock) {
            val again = cached
            if (again != null && !again.isExpired()) return again
            val fresh = acquireOAuthToken(provider)
            cached = fresh
            return fresh
        }
    }

    override fun authorizationHeader(): String = "Bearer " + currentToken().accessToken
}

/**
 * Process-wide registry mapping a WSDL endpoint to the [Authenticator] that should be used for it.
 * This lets the many existing request call sites (which only carry username/password) transparently
 * use OAuth without changing their signatures. When no authenticator is registered for an endpoint,
 * callers fall back to [BasicAuthenticator] built from the threaded username/password.
 */
object AuthenticatorRegistry {
    private val byEndpoint = ConcurrentHashMap<String, Authenticator>()

    fun register(endpoint: String, authenticator: Authenticator) {
        byEndpoint[endpoint] = authenticator
    }

    fun get(endpoint: String): Authenticator? = byEndpoint[endpoint]

    fun unregister(endpoint: String) {
        byEndpoint.remove(endpoint)
    }
}
