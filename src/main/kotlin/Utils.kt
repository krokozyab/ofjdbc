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
import java.sql.ResultSet
import java.sql.SQLException
import java.util.Base64
import javax.xml.parsers.DocumentBuilderFactory
import javax.xml.transform.OutputKeys
import javax.xml.transform.TransformerFactory
import javax.xml.transform.dom.DOMSource
import javax.xml.transform.stream.StreamResult
import my.jdbc.wsdl_driver.SecuredViewMappings

val logger = LoggerFactory.getLogger("Utils")

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

fun parseXml(xml: String): Document {
    val factory = DocumentBuilderFactory.newInstance().apply { isNamespaceAware = true }
    val builder = factory.newDocumentBuilder()

    fun tryParse(text: String): Document =
        builder.parse(InputSource(StringReader(text)))

    // First, try to parse the XML as-is
    return try {
        tryParse(xml)
    } catch (e: org.xml.sax.SAXParseException) {
        // Only apply cleaning if the initial parse fails
        parseXmlWithCleaning(xml)
    }
}

private fun parseXmlWithCleaning(xml: String): Document {
    //  Pre‑clean: escape stray '&' that are not part of an entity
    var candidate = xml.replace(
        Regex("&(?!amp;|lt;|gt;|quot;|apos;|#\\d+;|#x[0-9a-fA-F]+;)"),
        "&amp;"
    )

    //   Remove leading BOM/whitespace and any bytes before first '<'
    candidate = candidate.trimStart('\uFEFF', ' ', '\t', '\r', '\n')
    val firstLt = candidate.indexOf('<')
    if (firstLt > 0) candidate = candidate.substring(firstLt)

    //  Collapse illegal spaces before '=' in attributes
    candidate = candidate.replace(Regex("\\s+=\\s*"), "=")

    // Force‑wrap any element whose text still contains raw '<' or '>'
    // But skip if content looks like XML
    candidate = Regex(
        "<([A-Za-z][A-Za-z0-9_]*?)>([^<]*[<>][^<]*?)</\\1>",
        setOf(RegexOption.IGNORE_CASE, RegexOption.DOT_MATCHES_ALL)
    ).replace(candidate) { m ->
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

    fun escapeOutsideTags(text: String): String {
        val entity = Regex("&(amp|lt|gt|quot|apos|#\\d+;|#x[0-9a-fA-F]+;)")
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

    fun tryParse(text: String): Document =
        builder.parse(InputSource(StringReader(text)))

    //  Fast path – try to parse as‑is
    return try {
        tryParse(candidate)
    } catch (e: org.xml.sax.SAXParseException) {
        // --- second‑pass sanitiser: escape < and > that occur *between* tags
        val tagPlusText = Regex("(<[^>]+>)([^<]*[<>][^<]*)(?=<)").replace(candidate) { m ->
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
            Regex("\\s<([A-Za-z][A-Za-z0-9_]{1,25})(?=[\\s<]|$)"),
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
    val writer = StringWriter()
    transformer.transform(DOMSource(doc), StreamResult(writer))
    return writer.toString()
}

fun sendSqlViaWsdl(
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
    // Build a Java HttpClient (Java 11+)
    val client = HttpClient.newHttpClient()
    val request = HttpRequest.newBuilder()
        .uri(URI.create(wsdlEndpoint))
        .header("Content-Type", "application/soap+xml;charset=UTF-8")
        .header("SOAPAction", "#POST")
        .header("Authorization", authHeader)
        .header("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/114.0.5735.133 Safari/537.36")
        .POST(HttpRequest.BodyPublishers.ofString(soapEnvelope))
        .build()
    // Send the request synchronously and obtain the response as a String
    val response = client.send(request, HttpResponse.BodyHandlers.ofString())
    val status = response.statusCode()
    val body = response.body()
    // Check if the response is an HTML error page
    if (body.trim().startsWith("<html", ignoreCase = true) ||
        body.trim().startsWith("<!DOCTYPE html", ignoreCase = true)) {
        logger.error("Received an HTML error response (HTTP {}): {}", status, body)
        throw SQLException("WSDL service error ($status): Received an HTML error response. Details: $body")
    }
    if (body.isBlank()) {
        throw SQLException("Empty SOAP response from WSDL service. HTTP Status: $status")
    }
    val doc = parseXml(body)
    if (status == 200) {
        val reportNode = findNodeEndingWith(doc.documentElement, "reportBytes")
        if (reportNode != null) {
            val base64Str = reportNode.textContent.trim()
            return decodeBase64(base64Str)
        } else {
            throw SQLException("Invalid response format: No reportBytes found")
        }
    } else {
        val errorMessage = extractSoapFaultReason(body)
        throw SQLException("WSDL service error ($status): $errorMessage")
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
