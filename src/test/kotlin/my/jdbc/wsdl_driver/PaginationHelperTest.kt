package my.jdbc.wsdl_driver

import io.mockk.every
import io.mockk.mockkStatic
import io.mockk.unmockkStatic
import kotlin.test.Test
import kotlin.test.assertEquals

class PaginationHelperTest {
    @Test
    fun `fetchAllPages accumulates all rows`() {
        mockkStatic("my.jdbc.wsdl_driver.UtilsKt")
        val page1 = "<ROWSET><ROW><A>1</A></ROW></ROWSET>"
        val page2 = "<ROWSET><ROW><A>2</A></ROW></ROWSET>"
        val empty = "<ROWSET/>"
        every { sendSqlViaWsdl(any(), any(), any(), any(), any()) } returnsMany listOf(page1, page2, empty)

        val rows = fetchAllPages(
            wsdlEndpoint = "http://x",
            sql = "select * from dual",
            username = "u",
            password = "p",
            reportPath = "/r",
            fetchSize = 1,
            parsePage = { xml ->
                val doc = parseXml(xml)
                val list = doc.getElementsByTagName("ROW")
                val res = mutableListOf<Map<String,String>>()
                for (i in 0 until list.length) {
                    val node = list.item(i)
                    val map = mutableMapOf<String,String>()
                    val children = node.childNodes
                    for (j in 0 until children.length) {
                        val child = children.item(j)
                        if (child.nodeType == org.w3c.dom.Node.ELEMENT_NODE) {
                            map[child.nodeName.lowercase()] = child.textContent.trim()
                        }
                    }
                    res.add(map)
                }
                res
            }
        )
        assertEquals(listOf("1","2"), rows.map { it["a"] })
        unmockkStatic("my.jdbc.wsdl_driver.UtilsKt")
    }
}
