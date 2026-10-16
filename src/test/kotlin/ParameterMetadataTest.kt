package my.jdbc.wsdl_driver

import org.junit.jupiter.api.Test
import org.junit.jupiter.api.Assertions.*
import java.sql.ParameterMetaData
import java.sql.Types

class ParameterMetadataTest {
    
    @Test
    fun `SimpleParameterMetaData returns correct parameter count`() {
        val metadata = SimpleParameterMetaData(5)
        assertEquals(5, metadata.parameterCount)
    }

    @Test
    fun `SimpleParameterMetaData returns VARCHAR type for all parameters`() {
        val metadata = SimpleParameterMetaData(3)
        for (i in 1..3) {
            assertEquals(Types.VARCHAR, metadata.getParameterType(i))
            assertEquals("VARCHAR", metadata.getParameterTypeName(i))
            assertEquals("java.lang.String", metadata.getParameterClassName(i))
        }
    }

    @Test
    fun `SimpleParameterMetaData returns parameterModeIn for all parameters`() {
        val metadata = SimpleParameterMetaData(2)
        for (i in 1..2) {
            assertEquals(ParameterMetaData.parameterModeIn, metadata.getParameterMode(i))
        }
    }

    @Test
    fun `SimpleParameterMetaData returns parameterNullableUnknown for all parameters`() {
        val metadata = SimpleParameterMetaData(2)
        for (i in 1..2) {
            assertEquals(ParameterMetaData.parameterNullableUnknown, metadata.isNullable(i))
        }
    }

    @Test
    fun `SimpleParameterMetaData returns false for isSigned`() {
        val metadata = SimpleParameterMetaData(2)
        for (i in 1..2) {
            assertFalse(metadata.isSigned(i))
        }
    }

    @Test
    fun `SimpleParameterMetaData returns zero precision and scale`() {
        val metadata = SimpleParameterMetaData(2)
        for (i in 1..2) {
            assertEquals(0, metadata.getPrecision(i))
            assertEquals(0, metadata.getScale(i))
        }
    }

    @Test
    fun `SimpleParameterMetaData supports unwrap for same type`() {
        val metadata = SimpleParameterMetaData(1)
        val unwrapped = metadata.unwrap(SimpleParameterMetaData::class.java)
        assertSame(metadata, unwrapped)
    }

    @Test
    fun `SimpleParameterMetaData returns true for isWrapperFor same type`() {
        val metadata = SimpleParameterMetaData(1)
        assertTrue(metadata.isWrapperFor(SimpleParameterMetaData::class.java))
    }

    @Test
    fun `SimpleParameterMetaData handles zero parameters`() {
        val metadata = SimpleParameterMetaData(0)
        assertEquals(0, metadata.parameterCount)
    }
}
