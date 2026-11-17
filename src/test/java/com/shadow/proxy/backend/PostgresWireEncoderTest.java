package com.shadow.proxy.backend;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.UnpooledByteBufAllocator;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.nio.charset.StandardCharsets;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

class PostgresWireEncoderTest {
    @ParameterizedTest
    @CsvSource({"INSERT,INSERT 0 3", "UPDATE,UPDATE 3", "DELETE,DELETE 3"})
    void updateTagsMatchPostgresWireFormat(String type, String expected) {
        ByteBuf response = PostgresWireEncoder.encodeUpdateResult(UnpooledByteBufAllocator.DEFAULT, type, 3);
        try {
            assertEquals(expected + '\0', frame(response, 'C').toString(StandardCharsets.UTF_8));
            assertEquals("I", frame(response, 'Z').toString(StandardCharsets.UTF_8));
            assertFalse(response.isReadable());
        } finally { response.release(); }
    }

    @Test
    void errorContainsSeveritySqlStateMessageAndReadyForQuery() {
        ByteBuf response = PostgresWireEncoder.encodeError(UnpooledByteBufAllocator.DEFAULT,
                "ERROR", "42601", "bad SQL");
        try {
            String body = frame(response, 'E').toString(StandardCharsets.UTF_8);
            assertTrue(body.contains("SERROR\0"));
            assertTrue(body.contains("C42601\0"));
            assertTrue(body.contains("Mbad SQL\0"));
            assertEquals("I", frame(response, 'Z').toString(StandardCharsets.UTF_8));
            assertFalse(response.isReadable());
        } finally { response.release(); }
    }

    @Test
    void rowsPreserveUtf8AndSqlNull() throws Exception {
        ResultSet rs = mock(ResultSet.class);
        ResultSetMetaData metadata = mock(ResultSetMetaData.class);
        when(rs.getMetaData()).thenReturn(metadata);
        when(metadata.getColumnCount()).thenReturn(2);
        when(metadata.getColumnName(1)).thenReturn("name");
        when(metadata.getColumnName(2)).thenReturn("optional");
        when(rs.next()).thenReturn(true, false);
        when(rs.getString(1)).thenReturn("café");
        when(rs.getString(2)).thenReturn(null);
        ByteBuf response = PostgresWireEncoder.encodeResultSet(UnpooledByteBufAllocator.DEFAULT, rs);
        try {
            assertEquals(2, frame(response, 'T').readShort());
            ByteBuf row = frame(response, 'D');
            assertEquals(2, row.readShort());
            assertEquals("café", row.readCharSequence(row.readInt(), StandardCharsets.UTF_8).toString());
            assertEquals(-1, row.readInt());
            assertFalse(row.isReadable());
            assertEquals("SELECT 1\0", frame(response, 'C').toString(StandardCharsets.UTF_8));
            assertEquals("I", frame(response, 'Z').toString(StandardCharsets.UTF_8));
            assertFalse(response.isReadable());
        } finally { response.release(); }
    }

    private ByteBuf frame(ByteBuf response, char type) {
        assertEquals(type, response.readByte());
        int length = response.readInt();
        assertTrue(length >= 4 && length - 4 <= response.readableBytes());
        return response.readSlice(length - 4);
    }
}
