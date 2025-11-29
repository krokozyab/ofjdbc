package my.jdbc.wsdl_driver

import org.slf4j.LoggerFactory
import org.w3c.dom.Document
import org.w3c.dom.Element
import org.w3c.dom.Node
import org.w3c.dom.NodeList
import org.xml.sax.InputSource
import java.io.StringReader
import java.io.StringWriter
import java.net.URI
import java.net.http.HttpClient
import java.net.http.HttpRequest
import java.net.http.HttpResponse
import java.time.Duration
import java.sql.ResultSet
import java.sql.SQLException
import java.util.Base64
import javax.xml.parsers.DocumentBuilderFactory
import javax.xml.transform.OutputKeys
import javax.xml.transform.TransformerFactory
import javax.xml.transform.dom.DOMSource
import javax.xml.transform.stream.StreamResult
import javax.xml.transform.dom.DOMResult
import javax.xml.transform.stax.StAXSource
import javax.xml.stream.XMLInputFactory
import javax.xml.stream.XMLStreamReader
import org.ccil.cowan.tagsoup.Parser
import org.xml.sax.InputSource as SaxInputSource
import my.jdbc.wsdl_driver.SecuredViewMappings
import org.apache.commons.text.StringEscapeUtils
import javax.xml.stream.XMLStreamConstants
import java.io.ByteArrayInputStream
import java.util.zip.GZIPInputStream


/**
 * Conservative sanitizer that repairs common malformed XML fragments before parsing.
 * It performs a small set of safe transforms:
 *  - escapes stray '&' that are not entities
 *  - closes unclosed start-tags when immediately followed by another tag
 *  - fills bare attributes inside tags with a dummy value (attr="true")
 *  - truncates trailing non-XML garbage after the final closing tag
 */
fun sanitizeXmlGeneral(input: String): String {
    var s = input

    // 1) Escape stray ampersands that are not part of entities
    s = Regex("&(?!amp;|lt;|gt;|quot;|apos;|#[0-9]+;|#x[0-9a-fA-F]+;)").replace(s) { "&amp;" }

    // 2) Close start-tags that are immediately followed by another '<' (possibly after whitespace/newline)
    s = Regex("""<([A-Za-z][A-Za-z0-9_:-]*)\s*(?=\s*<)""").replace(s) { m ->
        val tag = m.groupValues[1]
        "<$tag></$tag>"
    }

    // 3) Fill bare attributes inside tags with a dummy value
    try {
        val tagRegex = Regex("<[^>]+>")
        s = tagRegex.replace(s) { tm ->
            var t = tm.value
            // normalize internal whitespace
            t = t.replace("\n", " ")
            val innerRe = Regex("""\s([A-Za-z_:][A-Za-z0-9_.:-]*)(?!\s*=)""")
            t = innerRe.replace(t) { it -> " ${it.groupValues[1]}=\"true\"" }
            t
        }
    } catch (_: Exception) {}

    // 4) Truncate trailing non-XML after last closing tag
    try {
        val lastClose = s.lastIndexOf("</")
        if (lastClose >= 0) {
            val gt = s.indexOf('>', lastClose)
            if (gt > lastClose) s = s.substring(0, gt + 1)
        }
    } catch (_: Exception) {}

    return s
}

val logger = LoggerFactory.getLogger("Utils")





// Reusable HttpClient - created once and reused with proper resource management
object HttpClientManager {
    @Volatile
    private var _httpClient: HttpClient? = null
    private val clientLock = Any()
    private var shutdownHookRegistered = false
    
    val httpClient: HttpClient
        get() {
            return _httpClient ?: synchronized(clientLock) {
                _httpClient ?: createHttpClient().also {
                    _httpClient = it
                    registerShutdownHook()
                }
            }
        }
    
    private fun createHttpClient(): HttpClient {
        return HttpClient.newBuilder()
            .connectTimeout(java.time.Duration.ofSeconds(30))
            .build()
    }
    
    private fun registerShutdownHook() {
        if (!shutdownHookRegistered) {
            Runtime.getRuntime().addShutdownHook(Thread {
                logger.info("Shutting down HttpClientManager")
                close()
            })
            shutdownHookRegistered = true
        }
    }
    
    fun close() {
        synchronized(clientLock) {
            _httpClient?.let { client ->
                try {
                    // HttpClient doesn't have explicit close method in Java 11+
                    // Resources are managed by the JVM
                    logger.info("HttpClient resources released")
                } catch (ex: Exception) {
                    logger.error("Error releasing HttpClient resources: ${ex.message}")
                } finally {
                    _httpClient = null
                }
            }
        }
    }
}

// Per-request timeout; configurable via env var OFJDBC_HTTP_TIMEOUT_SECONDS
private val requestTimeoutSeconds: Long =
    System.getenv("OFJDBC_HTTP_TIMEOUT_SECONDS")?.toLongOrNull() ?: 120L

fun encodeCredentials(username: String, password: String): String =
    "Basic " + Base64.getEncoder().encodeToString("$username:$password".toByteArray(Charsets.UTF_8))

fun decodeBase64(encoded: String): String {
    return try {
        val bytes = Base64.getDecoder().decode(encoded.toByteArray(Charsets.UTF_8))
        String(bytes, Charsets.UTF_8)
    } catch (e: Exception) {
        throw Exception("Error decoding Base64 response", e)
    }
}

fun createSoapEnvelope(sql: String, reportPath: String): String {
    return """
        <?xml version="1.0" encoding="UTF-8"?>
        <soap:Envelope xmlns:soap="http://www.w3.org/2003/05/soap-envelope" 
                       xmlns:pub="http://xmlns.oracle.com/oxp/service/PublicReportService">
          <soap:Body>
             <pub:runReport>
                <pub:reportRequest>
                   <pub:attributeFormat>xml</pub:attributeFormat>
                   <pub:byPassCache>true</pub:byPassCache>
                   <pub:reportAbsolutePath>$reportPath</pub:reportAbsolutePath>
                   <pub:sizeOfDataChunkDownload>-1</pub:sizeOfDataChunkDownload>
                   <pub:parameterNameValues>
                      <pub:item>
                         <pub:name>p_sql</pub:name>
                         <pub:values>
                            <pub:item><![CDATA[$sql]]></pub:item>
                         </pub:values>
                      </pub:item>
                   </pub:parameterNameValues>
                </pub:reportRequest>
             </pub:runReport>
          </soap:Body>
        </soap:Envelope>
    """.trimIndent()
}

fun extractSoapError(body: String): String {
    return try {
        val doc = parseXml(body)
        val faultNodes = doc.getElementsByTagName("Fault")
        if (faultNodes.length > 0) {
            val fault = faultNodes.item(0) as Element
            val faultStringNodes = fault.getElementsByTagName("faultstring")
            val faultCodeNodes = fault.getElementsByTagName("faultcode")
            val faultString = if (faultStringNodes.length > 0) faultStringNodes.item(0).textContent else "Unknown"
            val faultCode = if (faultCodeNodes.length > 0) faultCodeNodes.item(0).textContent else "Unknown"
            "SOAP Fault - Code: $faultCode, Message: $faultString"
        } else {
            "Unknown SOAP Fault"
        }
    } catch(e: Exception) {
        "Unknown SOAP Fault"
    }
}

fun extractSoapFaultReason(body: String): String {
    return try {
        val doc = parseXml(body)
        // Use the SOAP 1.2 namespace to find the <env:Text> element.
        val reasonNodes = doc.getElementsByTagNameNS("http://www.w3.org/2003/05/soap-envelope", "Text")
        if (reasonNodes.length > 0) {
            reasonNodes.item(0).textContent.trim()
        } else {
            "No fault reason found"
        }
    } catch (e: Exception) {
        "Error parsing SOAP fault: ${e.message}"
    }
}

/**
 * Decompress HTTP response bytes if gzip-compressed, otherwise decode as UTF-8 string.
 */
fun decompressResponseIfNeeded(bytes: ByteArray): String {
    // Check for gzip magic number (0x1f 0x8b)
    if (bytes.size >= 2 && bytes[0] == 0x1f.toByte() && bytes[1] == 0x8b.toByte()) {
        logger.debug("Response is gzip-compressed, decompressing...")
        return try {
            ByteArrayInputStream(bytes).use { bais ->
                GZIPInputStream(bais).use { gzis ->
                    gzis.readBytes().toString(Charsets.UTF_8)
                }
            }
        } catch (e: Exception) {
            logger.warn("Failed to decompress gzip response: ${e.message}")
            // Fall back to treating as UTF-8 string
            bytes.toString(Charsets.UTF_8)
        }
    }

    // Not gzip, decode as UTF-8 string
    return bytes.toString(Charsets.UTF_8)
}

/**
 * Safely decode an HTTP error response body, handling HTML content, SOAP faults, and binary data.
 * Returns a human-readable error message. Note: gzip decompression should be handled before calling this.
 */
fun decodeErrorResponse(body: String, statusCode: Int, maxLength: Int = 2000): String {
    if (body.isBlank()) {
        return "Empty response"
    }

    val trimmed = body.trim()

    // Check if it's HTML
    if (trimmed.startsWith("<html", ignoreCase = true) ||
        trimmed.startsWith("<!DOCTYPE html", ignoreCase = true)) {
        // Try to extract title or meaningful text from HTML
        val titleMatch = Regex("<title>([^<]+)</title>", RegexOption.IGNORE_CASE).find(body)
        val title = titleMatch?.groupValues?.get(1)?.trim() ?: "HTML Error Page"

        // Try to extract error message from common Oracle error patterns
        val h1Match = Regex("<h1>([^<]+)</h1>", RegexOption.IGNORE_CASE).find(body)
        val h1Text = h1Match?.groupValues?.get(1)?.trim()

        val message = when {
            h1Text != null && h1Text.isNotBlank() -> h1Text
            title != "HTML Error Page" -> title
            else -> "Received HTML error page"
        }

        return "HTTP $statusCode: $message"
    }

    // Check if it's a SOAP fault
    if (trimmed.contains("Fault", ignoreCase = true) &&
        (trimmed.contains("soap", ignoreCase = true) || trimmed.contains("envelope", ignoreCase = true))) {
        return try {
            // First try to extract the fault reason using the proper SOAP namespace
            val faultReason = extractSoapFaultReason(body)
            if (faultReason != "No fault reason found" && !faultReason.startsWith("Error parsing")) {
                // Try to extract just Oracle errors from the fault reason
                val oracleErrors = extractOracleErrors(faultReason)
                if (oracleErrors != null) {
                    return oracleErrors
                }
                return faultReason
            }

            // If that didn't work, try the generic SOAP error extractor
            val soapError = extractSoapError(body)
            if (soapError != "Unknown SOAP Fault") {
                // Try to extract just Oracle errors
                val oracleErrors = extractOracleErrors(soapError)
                if (oracleErrors != null) {
                    return oracleErrors
                }
                return soapError
            }

            // Last resort: extract text content from env:Text or faultstring elements using regex
            // Use DOTALL to capture multi-line content
            val textMatch = Regex("""<env:Text[^>]*>(.*?)</env:Text>""", setOf(RegexOption.IGNORE_CASE, RegexOption.DOT_MATCHES_ALL)).find(body)
                ?: Regex("""<faultstring>(.*?)</faultstring>""", setOf(RegexOption.IGNORE_CASE, RegexOption.DOT_MATCHES_ALL)).find(body)

            if (textMatch != null) {
                val errorText = textMatch.groupValues[1].trim()
                // Clean up any XML entities
                val decodedText = errorText.replace("&lt;", "<")
                    .replace("&gt;", ">")
                    .replace("&amp;", "&")
                    .replace("&quot;", "\"")
                    .replace("&apos;", "'")

                // Try to extract just Oracle errors
                val oracleErrors = extractOracleErrors(decodedText)
                if (oracleErrors != null) {
                    return oracleErrors
                }
                return decodedText
            } else {
                // If all parsing fails, just return the body (don't truncate SOAP faults as they contain important error info)
                return body
            }
        } catch (e: Exception) {
            logger.warn("Failed to parse SOAP fault: ${e.message}")
            body
        }
    }

    // Check for Oracle errors in plain text responses
    val oracleErrors = extractOracleErrors(trimmed)
    if (oracleErrors != null) {
        return oracleErrors
    }

    // Plain text or unknown format - truncate if too long
    return truncate(trimmed, maxLength)
}

/**
 * Extract Oracle error codes and messages from a verbose error message,
 * removing Java exception chain noise.
 */
fun extractOracleErrors(message: String): String? {
    // Look for ORA-XXXXX error codes in the message
    val oraPattern = Regex("""(ORA-\d{5}:.*?)(?=ORA-\d{5}:|$)""", setOf(RegexOption.DOT_MATCHES_ALL))
    val matches = oraPattern.findAll(message).toList()

    if (matches.isEmpty()) {
        return null
    }

    // Clean up each ORA error line
    val cleanErrors = matches.map { match ->
        val error = match.groupValues[1].trim()
        // Remove any trailing URL or other noise after the error message
        error.replace(Regex("""https?://[^\s]+"""), "").trim()
    }

    return cleanErrors.joinToString("\n")
}

/**
 * Truncate a string to a maximum length, adding ellipsis if truncated
 */
private fun truncate(text: String, maxLength: Int): String {
    if (text.length <= maxLength) return text
    return text.take(maxLength) + "... (truncated)"
}

fun parseXml(xml: String): Document {
    // Universal parsing pipeline:
    // 1) Try strict DOM parse
    // 2) If fails, try TagSoup (for badly-formed markup)
    // 3) If still fails, try lightweight cleaning heuristics and retry DOM
    // 4) If still fails, try StAX -> DOM as last resort

    val factory = DocumentBuilderFactory.newInstance().apply { isNamespaceAware = true }
    val builder = factory.newDocumentBuilder()

    // Helper to try a DOM parse and wrap exceptions
    fun tryDom(text: String): Document {
        return StringReader(text).use { reader -> builder.parse(InputSource(reader)) }
    }

    // Pre-sanitize the input to repair common malformations before attempting strict DOM.
    val pre = sanitizeXmlGeneral(xml)

    // 1) Try strict DOM on sanitized input
    try {
        return tryDom(pre)
    } catch (_: Exception) {
        // continue to fallback
    }

    // 2) Try TagSoup SAX -> DOM (very forgiving)
    try {
        val parser = org.ccil.cowan.tagsoup.Parser()
        val saxSource = javax.xml.transform.sax.SAXSource(parser, org.xml.sax.InputSource(StringReader(pre)))
        val doc = builder.newDocument()
        javax.xml.transform.TransformerFactory.newInstance().newTransformer().transform(saxSource, DOMResult(doc))
        return doc
    } catch (_: Exception) {
        // continue to next fallback
    }

    // 3) Try existing cleaning heuristics then DOM
    try {
        val cleaned = parseXmlWithCleaning(pre)
        return cleaned
    } catch (_: Exception) {
        // continue to StAX fallback
    }

    // 4) StAX -> DOM via Transformer
    try {
        val xif = XMLInputFactory.newFactory()
        try { xif.setProperty(XMLInputFactory.IS_COALESCING, java.lang.Boolean.TRUE) } catch (_: Exception) {}
        try { xif.setProperty(XMLInputFactory.SUPPORT_DTD, java.lang.Boolean.FALSE) } catch (_: Exception) {}
        try { xif.setProperty("javax.xml.stream.isSupportingExternalEntities", java.lang.Boolean.FALSE) } catch (_: Exception) {}
        val xmlReader = xif.createXMLStreamReader(StringReader(pre))
        try {
            val source = StAXSource(xmlReader)
            val doc = builder.newDocument()
            javax.xml.transform.TransformerFactory.newInstance().newTransformer().transform(source, DOMResult(doc))
            return doc
        } finally {
            try { xmlReader.close() } catch (_: Exception) {}
        }
    } catch (e: Exception) {
        // if everything fails, rethrow last error as SAXParseException for callers
        throw org.xml.sax.SAXParseException(e.message ?: "parse error", null)
    }
}

fun parseXmlWithCleaning(xml: String): Document {
    //  Pre‑clean: escape stray '&' that are not part of an entity
    var candidate = xml.replace(
        Regex("&(?!amp;|lt;|gt;|quot;|apos;|#\\d+;|#x[0-9a-fA-F]+;)"),
        "&amp;"
    )

    //   Remove leading BOM/whitespace and any bytes before first '<'
    candidate = candidate.trimStart('\uFEFF', ' ', '\t', '\r', '\n')
    val firstLt = candidate.indexOf('<')
    if (firstLt > 0) candidate = candidate.substring(firstLt)


    // Sanitize bare attribute names inside start-tags (e.g. <ROWSET attribute_without_value >)
    // by adding a dummy value. We only perform this inside tag text to avoid touching normal content.
    try {
        val tagRegex = Regex("<[^>]+>")
        candidate = tagRegex.replace(candidate) { tagMatch ->
            var t = tagMatch.value
            // Replace newlines inside tag with spaces to simplify matching
            t = t.replace("\n", " ")
            // Use double-escaped backslashes in Kotlin string literals so the
            // regex engine receives single backslashes (e.g. \s for whitespace).
            val innerRe = Regex("\\s([A-Za-z_:][A-Za-z0-9_.:-]*)(?!\\s*=)")
            t = innerRe.replace(t) { it -> " ${it.groupValues[1]}=\"true\"" }
            t
        }
    } catch (_: Exception) {}

    // If there is extraneous markup after the root element (e.g. trailing text), truncate
    // to the last closing tag to avoid "markup after root" parse errors.
    try {
        val lastClose = candidate.lastIndexOf("</")
        if (lastClose >= 0) {
            val gt = candidate.indexOf('>', lastClose)
            if (gt > lastClose && gt < candidate.length - 1) {
                // truncate after the final closing tag
                candidate = candidate.substring(0, gt + 1)
            }
        }
    } catch (_: Exception) {}


    // Fix common malformed pattern: a start tag missing its closing '>' that is
    // immediately followed by another '<' (e.g. "<area_id
// <DESCRIPTION...").
    // Convert "<tag <..." -> "<tag></tag><..." so the parser can continue.
    candidate = Regex("""<([A-Za-z][A-Za-z0-9_-]*)\s*(?=\s*<)""").replace(candidate) { m ->
        val tag = m.groupValues[1]
        "<${tag}></${tag}>"
    }

    //  Collapse illegal spaces before '=' in attributes
    candidate = candidate.replace(Regex("""\s+=\s*"""), "=")

    // Force‑wrap any element whose text still contains raw '<' or '>'
    // But skip if content looks like XML
    candidate = Regex("<([A-Za-z][A-Za-z0-9_]*?)>([^<]*[<>][^<]*?)</\\1>", setOf(RegexOption.IGNORE_CASE, RegexOption.DOT_MATCHES_ALL)).replace(candidate) { m ->
        val tag = m.groupValues[1]
        val body = m.groupValues[2]

        // Skip CDATA wrapping if content looks like XML
        val looksLikeXml = body.contains("</") || body.matches(Regex(".*<[A-Za-z][^>]*>.*"))

        if (looksLikeXml) {
            m.value // Keep original
        } else {
            val safeBody = body.replace("]]>", "]]]]><![CDATA[>") // keep CDATA safe
            "<$tag><![CDATA[$safeBody]]></$tag>"
        }
    }

    val factory = DocumentBuilderFactory.newInstance().apply { isNamespaceAware = true }
    val builder = factory.newDocumentBuilder()

    fun tryParse(text: String): Document {
        return StringReader(text).use { reader ->
            builder.parse(InputSource(reader))
        }
    }

    fun escapeOutsideTags(text: String): String {
        val entity = Regex("""&(amp|lt|gt|quot|apos|#\d+;|#x[0-9a-fA-F]+;)""")
        val sb = StringBuilder()
        var i = 0
        var inTag = false
        while (i < text.length) {
            val c = text[i]
            when (c) {
                '<' -> {
                    val next = if (i + 1 < text.length) text[i + 1] else '\u0000'
                    val looksLikeTag = next.isLetterOrDigit() || next == '/' || next == '!' || next == '?'
                    if (!inTag && !looksLikeTag) {
                        sb.append("&lt;")
                        i++
                        continue
                    }
                    inTag = true
                    sb.append('<')
                }
                '>' -> {
                    inTag = false
                    sb.append('>')
                }
                '&' -> {
                    if (!inTag) {
                        val semi = text.indexOf(';', i)
                        if (semi > i && entity.matches(text.substring(i, semi + 1))) {
                            sb.append(text.substring(i, semi + 1))
                            i = semi
                        } else {
                            sb.append("&amp;")
                        }
                    } else {
                        sb.append('&')
                    }
                }
                else -> {
                    if (!inTag) {
                        when (c) {
                            '<' -> sb.append("&lt;")
                            '>' -> sb.append("&gt;")
                            else -> sb.append(c)
                        }
                    } else sb.append(c)
                }
            }
            i++
        }
        return sb.toString()
    }

    // Final general sanitizer to repair broad classes of malformed tags
    

    //  Fast path – try to parse as‑is
    return try {
        tryParse(candidate)
    } catch (e: org.xml.sax.SAXParseException) {
        // --- second‑pass sanitiser: escape < and > that occur *between* tags
        val tagPlusText = Regex("""(<[^>]+>)([^<]*[<>][^<]*)(?=\s*<)""").replace(candidate) { m ->
            val openTag  = m.groupValues[1]
            val badText  = m.groupValues[2]
            val escaped  = badText.replace("<", "&lt;").replace(">", "&gt;")
            openTag + escaped
        }
        // second stage – escape any remaining stray '>' then orphan '<WORD'
        val withGtEscaped = Regex("([^>])>(?=[^<])").replace(tagPlusText) {
            it.groupValues[1] + "&gt;"
        }

        // Escape orphan "<WORD" that is preceded by whitespace (e.g. " NAME> <CUSTOMER")
        val sanitized = withGtEscaped.replace(
            Regex("""\s<([A-Za-z][A-Za-z0-9_]{1,25})(?=[\s<]|$)"""),
            " &lt;$1"
        )

        return try {
            tryParse(sanitized)
        } catch (_: org.xml.sax.SAXParseException) {
            val fallback = escapeOutsideTags(candidate)
            tryParse(fallback)
        }
    }
}

fun findNodeEndingWith(node: Node, suffix: String): Node? {
    if (node.nodeType == Node.ELEMENT_NODE && node.nodeName.endsWith(suffix)) return node
    val children = node.childNodes
    for (i in 0 until children.length) {
        val result = findNodeEndingWith(children.item(i), suffix)
        if (result != null) return result
    }
    return null
}

fun documentToString(doc: Document): String {
    val transformer = TransformerFactory.newInstance().newTransformer().apply {
        setOutputProperty(OutputKeys.INDENT, "yes")
    }
    return StringWriter().use { writer ->
        transformer.transform(DOMSource(doc), StreamResult(writer))
        writer.toString()
    }
}

/**
 * Execute an operation with retry logic and exponential backoff
 */
fun <T> executeWithRetry(
    operation: String,
    retryConfig: RetryConfig = RetryConfig.fromEnvironment(),
    block: () -> T
): T {
    val context = RetryContext(operation)
    var lastException: Exception? = null
    
    repeat(retryConfig.maxAttempts) { attemptIndex ->
        try {
            context.recordAttempt(null)
            val result = block()
            if (attemptIndex > 0) {
                logger.info("Operation '{}' succeeded on attempt {} after {}ms", 
                    operation, attemptIndex + 1, context.totalElapsedMs())
            }
            return result
        } catch (e: Exception) {
            lastException = e
            context.recordAttempt(e)
            
            // Check if this is an Oracle SQL error - these should never be retried
            val isOracleError = e.message?.let { msg ->
                msg.contains("Oracle SQL error") || msg.contains(Regex("""ORA-\d{5}"""))
            } == true

            val isRetryable = !isOracleError && (
                isRetryableException(e) ||
                (e is SQLException && e.message?.let { msg ->
                    val lowerMsg = msg.lowercase()
                    // Check for "retryable error" messages or HTTP status codes
                    lowerMsg.contains("retryable error") ||
                    (lowerMsg.contains("http") && (
                        lowerMsg.contains("500") || lowerMsg.contains("502") ||
                        lowerMsg.contains("503") || lowerMsg.contains("504")
                    ))
                } == true)
            )

            if (!isRetryable || attemptIndex == retryConfig.maxAttempts - 1) {
                logger.error("Operation '{}' failed after {} attempts in {}ms. Last error: {}",
                    operation, context.attempt, context.totalElapsedMs(), e.message)
                throw e
            }
            
            val delay = retryConfig.calculateDelay(attemptIndex)
            logger.warn("Operation '{}' failed on attempt {} ({}), retrying in {}ms", 
                operation, attemptIndex + 1, e.message, delay)
            
            try {
                Thread.sleep(delay)
            } catch (ie: InterruptedException) {
                Thread.currentThread().interrupt()
                throw SQLException("Retry interrupted", ie)
            }
        }
    }
    
    throw lastException ?: SQLException("Retry logic failed unexpectedly")
}

fun sendSqlViaWsdl(
    wsdlEndpoint: String,
    sql: String,
    username: String,
    password: String,
    reportPath: String
): String {
    return executeWithRetry("WSDL SQL Execution") {
        sendSqlViaWsdlInternal(wsdlEndpoint, sql, username, password, reportPath)
    }
}

private fun sendSqlViaWsdlInternal(
    wsdlEndpoint: String,
    sql: String,
    username: String,
    password: String,
    reportPath: String
): String {
    // Remove single-line (--) and multi-line (/* */) SQL comments
    val sqlWithoutComments = sql
        .replace(Regex("--.*?(\\r?\\n|$)"), " ") // single-line comments --
        .replace(Regex("/\\*.*?\\*/", RegexOption.DOT_MATCHES_ALL), " ") // multi-line comments /**/
    val normalizedSql = sqlWithoutComments.replace("\\s+".toRegex(), " ").trim()
    val securedSql = SecuredViewMappings.apply(normalizedSql)
    logger.info("Sending SQL to WSDL service: {}", securedSql)
    val soapEnvelope = createSoapEnvelope(securedSql, reportPath)
    val authHeader = encodeCredentials(username, password)
    
    val request = HttpRequest.newBuilder()
        .uri(URI.create(wsdlEndpoint))
        .header("Content-Type", "application/soap+xml;charset=UTF-8")
        .header("SOAPAction", "#POST")
        .header("Authorization", authHeader)
        .header("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/114.0.5735.133 Safari/537.36")
        .timeout(Duration.ofSeconds(requestTimeoutSeconds))
        .POST(HttpRequest.BodyPublishers.ofString(soapEnvelope))
        .build()
    
    // Receive response as bytes to preserve binary data (in case of gzip)
    val response = HttpClientManager.httpClient.send(request, HttpResponse.BodyHandlers.ofByteArray())
    val status = response.statusCode()
    val bodyBytes = response.body()

    if (bodyBytes.isEmpty()) {
        throw SQLException("Empty SOAP response from WSDL service. HTTP Status: $status")
    }

    // Decompress if gzip-compressed, otherwise decode as UTF-8 string
    val body = decompressResponseIfNeeded(bodyBytes)

    // Check for retryable HTTP status codes
    if (isRetryableHttpStatus(status)) {
        val readableError = decodeErrorResponse(body, status)

        // Check if this is an Oracle SQL error (ORA-XXXXX) - these are not retryable
        if (readableError.contains(Regex("""ORA-\d{5}"""))) {
            throw SQLException("Oracle SQL error ($status): $readableError")
        }

        throw SQLException("WSDL service returned retryable error ($status): $readableError")
    }

    // For successful responses, parse and extract reportBytes
    if (status == 200) {
        val doc = parseXml(body)
        val reportNode = findNodeEndingWith(doc.documentElement, "reportBytes")
        if (reportNode != null) {
            val base64Str = reportNode.textContent.trim()
            return decodeBase64(base64Str)
        } else {
            throw SQLException("Invalid response format: No reportBytes found")
        }
    } else {
        // For error responses, decode the error message properly
        val readableError = decodeErrorResponse(body, status)

        // Check if this is an Oracle SQL error - these are not retryable
        if (readableError.contains(Regex("""ORA-\d{5}"""))) {
            throw SQLException("Oracle SQL error ($status): $readableError")
        }

        throw SQLException("WSDL service error ($status): $readableError")
    }
}

fun createResultSetFromRowNodes(rowNodes: NodeList): ResultSet {
    // Map lower‑case → original‑case (first appearance wins) to preserve metadata names
    val originalByLc = linkedMapOf<String, String>()
    val rawRows      = mutableListOf<MutableMap<String, String>>()
    for (i in 0 until rowNodes.length) {
        val rowNode = rowNodes.item(i)
        if (rowNode.nodeType != Node.ELEMENT_NODE) continue
        val rowMap = linkedMapOf<String, String>()          // keeps insertion order of lc keys
        val children = rowNode.childNodes
        for (j in 0 until children.length) {
            val child = children.item(j)
            if (child.nodeType == Node.ELEMENT_NODE) {
                val original = child.nodeName               // as in XML
                val lc       = original.lowercase()
                // remember the original name only the first time we see this lc key
                originalByLc.putIfAbsent(lc, original)
                rowMap[lc] = child.textContent.trim()
            }
        }
        rawRows += rowMap
    }
    // Complete rows so each has every column
    val allLcCols = originalByLc.keys.toList()              // preserves first‑seen order
    rawRows.forEach { row -> allLcCols.forEach { row.putIfAbsent(it, "") } }
    val rows: List<Map<String, String>> = rawRows
    // Build ResultSet; XmlResultSet will derive metadata from the first row (keys are in stable order)
    return XmlResultSet(rows)
}

fun createEmptyResultSet(): ResultSet = XmlResultSet(emptyList())

/**
 * Parse all <ROW> elements from the provided XML using a StAX stream reader.
 * This is a streaming, memory-efficient alternative to DOM-based extraction
 * and supports nested escaped <RESULT> blocks containing XML.
 *
 * @param xml input XML text
 * @param lowercaseKeys if true, map element names to lowercase keys (default: false)
 */
fun parseRows(xml: String, lowercaseKeys: Boolean = false): List<Map<String, String>> {
    // Try streaming parse first; if the XML is malformed for StAX, fall back
    // to DOM-based cleaned parsing which contains more heuristics.
    try {
        val rows = mutableListOf<Map<String, String>>()
        val xif = XMLInputFactory.newFactory()
        try { xif.setProperty(XMLInputFactory.IS_COALESCING, java.lang.Boolean.TRUE) } catch (_: Exception) {}
        try { xif.setProperty(XMLInputFactory.SUPPORT_DTD, java.lang.Boolean.FALSE) } catch (_: Exception) {}
        try { xif.setProperty("javax.xml.stream.isSupportingExternalEntities", java.lang.Boolean.FALSE) } catch (_: Exception) {}

        val reader = xif.createXMLStreamReader(StringReader(xml))
        var currentRow: MutableMap<String, String>? = null
        var currentName: String? = null
        val textBuf = StringBuilder()

        fun finishElement(name: String) {
            if (currentRow == null || currentName == null) return
            val key = if (lowercaseKeys) currentName!!.lowercase() else currentName!!.uppercase()
            currentRow!![key] = textBuf.toString().trim()
            currentName = null
            textBuf.setLength(0)
        }

        try {
            while (reader.hasNext()) {
                when (reader.next()) {
                    XMLStreamConstants.START_ELEMENT -> {
                        val local = reader.localName
                        if (local.equals("ROW", ignoreCase = true)) {
                            currentRow = mutableMapOf()
                        } else if (currentRow != null) {
                            currentName = local
                            textBuf.setLength(0)
                        } else if (local.equals("RESULT", ignoreCase = true)) {
                            // capture the text inside RESULT (might be escaped XML)
                            val txt = reader.elementText
                            if (!txt.isNullOrBlank()) {
                                val unescaped = StringEscapeUtils.unescapeXml(txt)
                                rows.addAll(parseRows(unescaped, lowercaseKeys))
                            }
                        }
                    }
                    XMLStreamConstants.CHARACTERS, XMLStreamConstants.CDATA -> {
                        if (currentRow != null && currentName != null) textBuf.append(reader.text)
                    }
                    XMLStreamConstants.END_ELEMENT -> {
                        val local = reader.localName
                        if (local.equals("ROW", ignoreCase = true)) {
                            if (currentRow != null) {
                                rows.add(currentRow)
                                currentRow = null
                            }
                            currentName = null
                            textBuf.setLength(0)
                        } else if (currentRow != null && currentName != null && local.equals(currentName, ignoreCase = true)) {
                            finishElement(local)
                        }
                    }
                    else -> {}
                }
            }
        } finally {
            try { reader.close() } catch (_: Exception) {}
        }

        return rows
    } catch (ex: Exception) {
        logger.warn("StAX streaming parse failed (falling back to DOM cleaning): {}", ex.message)
        // Fallback: parse via the cleaning DOM path and extract ROW elements
        val doc = try {
            parseXmlWithCleaning(xml)
        } catch (e: Exception) {
            // As last resort, try the plain DOM parse
            parseXml(xml)
        }

        val result = mutableListOf<Map<String, String>>()
        val rowNodes = doc.getElementsByTagName("ROW")
        for (i in 0 until rowNodes.length) {
            val rn = rowNodes.item(i)
            if (rn.nodeType != Node.ELEMENT_NODE) continue
            val map = mutableMapOf<String, String>()
            val children = rn.childNodes
            for (j in 0 until children.length) {
                val child = children.item(j)
                if (child.nodeType != Node.ELEMENT_NODE) continue
                val name = if (lowercaseKeys) child.nodeName.lowercase() else child.nodeName.uppercase()
                map[name] = child.textContent.trim()
            }
            result.add(map)
        }

        // Also handle nested RESULT blocks if document had them (some callers expect recursive handling)
        if (rowNodes.length == 0) {
            val results = doc.getElementsByTagName("RESULT")
            for (k in 0 until results.length) {
                val txt = results.item(k).textContent.trim()
                if (txt.isNotBlank()) {
                    val unescaped = StringEscapeUtils.unescapeXml(txt)
                    result.addAll(parseRows(unescaped, lowercaseKeys))
                }
            }
        }

        return result
    }
}
