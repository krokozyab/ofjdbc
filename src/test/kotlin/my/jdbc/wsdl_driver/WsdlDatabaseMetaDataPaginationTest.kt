package my.jdbc.wsdl_driver

import io.mockk.every
import io.mockk.mockkStatic
import io.mockk.unmockkStatic
import kotlin.test.AfterTest
import kotlin.test.BeforeTest
import kotlin.test.Test
import kotlin.test.assertEquals

class WsdlDatabaseMetaDataPaginationTest {
    @BeforeTest
    fun setup() {
        mockkStatic("my.jdbc.wsdl_driver.UtilsKt")
        WsdlDatabaseMetaData.METADATA_FETCH_SIZE = 1
    }

    @AfterTest
    fun teardown() {
        unmockkStatic("my.jdbc.wsdl_driver.UtilsKt")
        WsdlDatabaseMetaData.METADATA_FETCH_SIZE = 1000
        LocalMetadataCache.clearAllCache()
    }

    @Test
    fun `getTables fetches multiple pages`() {
        val page1 = "<ROWSET><ROW><TABLE_NAME>T1</TABLE_NAME><TABLE_TYPE>TABLE</TABLE_TYPE></ROW></ROWSET>"
        val page2 = "<ROWSET><ROW><TABLE_NAME>T2</TABLE_NAME><TABLE_TYPE>TABLE</TABLE_TYPE></ROW></ROWSET>"
        val empty = "<ROWSET/>"
        every { sendSqlViaWsdl(any(), any(), any(), any(), any()) } returnsMany listOf(page1, page2, empty)

        val conn = WsdlConnection("http://x","u","p","/r")
        val meta = WsdlDatabaseMetaData(conn)
        val rs = meta.getTables(null, null, null, arrayOf("TABLE"))
        val names = mutableListOf<String>()
        while (rs.next()) {
            names.add(rs.getString("table_name"))
        }
        assertEquals(listOf("T1","T2"), names)
    }
}
