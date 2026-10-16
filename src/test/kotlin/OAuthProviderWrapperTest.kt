package my.jdbc.wsdl_driver

import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNotNull
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test

class OAuthProviderWrapperTest {

    /** Structurally compatible but does NOT implement OAuthProvider. */
    class DuckProvider {
        fun getClientId(): String = "cid"
        fun getClientSecret(): String = "secret"
        fun getTokenEndpoint(): String = "https://example.com/token"
        fun getScope(): String? = "scope-a"
        fun getGrantType(): String = "password"
        fun getClientAuthMethod(): String = "basic"
        fun getAdditionalParameters(): Map<String, String> = mapOf("audience" to "aud")
    }

    /** Only the required methods present; optional ones must fall back to defaults. */
    class MinimalDuckProvider {
        fun getClientId(): String = "cid"
        fun getClientSecret(): String = ""
        fun getTokenEndpoint(): String = "https://example.com/token"
    }

    class IncompatibleProvider {
        fun getClientId(): String = "cid"
        fun getTokenEndpoint(): Int = 42
    }

    @Test
    fun `wraps fully compatible object`() {
        val wrapper = OAuthProviderWrapper.wrapIfCompatible(DuckProvider())
        assertNotNull(wrapper)
        wrapper!!
        assertTrue(wrapper is OAuthProvider)
        assertEquals("cid", wrapper.getClientId())
        assertEquals("secret", wrapper.getClientSecret())
        assertEquals("https://example.com/token", wrapper.getTokenEndpoint())
        assertEquals("scope-a", wrapper.getScope())
        assertEquals("password", wrapper.getGrantType())
        assertEquals("basic", wrapper.getClientAuthMethod())
        assertEquals(mapOf("audience" to "aud"), wrapper.getAdditionalParameters())
    }

    @Test
    fun `applies defaults for missing optional methods`() {
        val wrapper = OAuthProviderWrapper.wrapIfCompatible(MinimalDuckProvider())
        assertNotNull(wrapper)
        wrapper!!
        assertNull(wrapper.getScope())
        assertEquals("client_credentials", wrapper.getGrantType())
        assertEquals("post", wrapper.getClientAuthMethod())
        assertEquals(emptyMap<String, String>(), wrapper.getAdditionalParameters())
    }

    @Test
    fun `rejects incompatible object and reports mismatches`() {
        val mismatches = mutableListOf<String>()
        assertNull(OAuthProviderWrapper.wrapIfCompatible(IncompatibleProvider(), mismatches))
        assertEquals(2, mismatches.size)
        assertTrue(mismatches.any { it.contains("getClientSecret") })
        assertTrue(mismatches.any { it.contains("getTokenEndpoint") })
    }

    @Test
    fun `wraps real OAuthProvider implementation too`() {
        val wrapper = OAuthProviderWrapper.wrapIfCompatible(
            SystemEnvOAuthProvider("https://host/wsdl", "u", "p")
        )
        assertNotNull(wrapper)
        assertEquals("u", wrapper!!.getClientId())
    }
}
