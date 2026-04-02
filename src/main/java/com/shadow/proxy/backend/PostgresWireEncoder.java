package com.shadow.proxy.backend;

import java.nio.charset.StandardCharsets;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;

/**
 * Encodes JDBC results into PostgreSQL wire protocol v3 messages.
 *
 * Message layout reference:
 *   RowDescription  'T'  → column names and type OIDs
 *   DataRow         'D'  → one message per row
 *   CommandComplete 'C'  → "SELECT 5", "INSERT 0 1", etc.
 *   ReadyForQuery   'Z'  → 'I' (idle)
 *   ErrorResponse   'E'  → severity + message fields
 */
public final class PostgresWireEncoder {

    // Text format (0) for all columns; OID 0 = unknown type (client infers)
    private static final int  TEXT_FORMAT = 0;
    private static final int  TYPE_OID    = 0;
    private static final int  TYPE_LEN    = -1;  // variable
    private static final int  TYPE_MOD    = -1;  // no modifier

    private PostgresWireEncoder() {}

    // ── ReadyForQuery ('Z') ──────────────────────────────────────────────────

    public static ByteBuf readyForQuery(ByteBufAllocator alloc) {
        ByteBuf buf = alloc.buffer(6);
        buf.writeByte('Z');
        buf.writeInt(5);
        buf.writeByte('I'); // idle
        return buf;
    }

    // ── Full SELECT response: RowDescription + DataRows + CommandComplete + Z ─

    public static ByteBuf encodeResultSet(ByteBufAllocator alloc, ResultSet rs)
            throws SQLException {
        ResultSetMetaData meta = rs.getMetaData();
        int colCount = meta.getColumnCount();
        ByteBuf buf  = alloc.buffer(256);

        // ── RowDescription ───────────────────────────────────────────────────
        // We write into a temp buf first so we know the total length
        ByteBuf td = alloc.buffer(64);
        td.writeShort(colCount);
        for (int i = 1; i <= colCount; i++) {
            writeNullString(td, meta.getColumnName(i));
            td.writeInt(0);           // table OID (0 = not from a table)
            td.writeShort(0);         // attribute number
            td.writeInt(TYPE_OID);    // data type OID
            td.writeShort(TYPE_LEN);  // type size
            td.writeInt(TYPE_MOD);    // type modifier
            td.writeShort(TEXT_FORMAT);
        }
        buf.writeByte('T');
        buf.writeInt(td.readableBytes() + 4); // +4 for length field itself
        buf.writeBytes(td);
        td.release();

        // ── DataRows ─────────────────────────────────────────────────────────
        int rowCount = 0;
        while (rs.next()) {
            ByteBuf row = alloc.buffer(64);
            row.writeShort(colCount);
            for (int i = 1; i <= colCount; i++) {
                String value = rs.getString(i);
                if (value == null) {
                    row.writeInt(-1); // NULL sentinel
                } else {
                    byte[] bytes = value.getBytes(StandardCharsets.UTF_8);
                    row.writeInt(bytes.length);
                    row.writeBytes(bytes);
                }
            }
            buf.writeByte('D');
            buf.writeInt(row.readableBytes() + 4);
            buf.writeBytes(row);
            row.release();
            rowCount++;
        }

        // ── CommandComplete ──────────────────────────────────────────────────
        writeCommandComplete(buf, alloc, "SELECT " + rowCount);

        // ── ReadyForQuery ────────────────────────────────────────────────────
        buf.writeBytes(readyForQuery(alloc));

        return buf;
    }

    // ── Non-SELECT (INSERT / UPDATE / DELETE / DDL) ──────────────────────────

    public static ByteBuf encodeUpdateResult(ByteBufAllocator alloc,
                                              String commandTag,
                                              int affectedRows) {
        ByteBuf buf = alloc.buffer(32);
        String tag  = switch (commandTag.toUpperCase()) {
            case "INSERT" -> "INSERT 0 " + affectedRows;
            case "UPDATE" -> "UPDATE " + affectedRows;
            case "DELETE" -> "DELETE " + affectedRows;
            default       -> commandTag; // DDL tags like "CREATE TABLE"
        };
        writeCommandComplete(buf, alloc, tag);
        buf.writeBytes(readyForQuery(alloc));
        return buf;
    }

    // ── ErrorResponse ('E') ──────────────────────────────────────────────────

    public static ByteBuf encodeError(ByteBufAllocator alloc,
                                       String severity,
                                       String sqlState,
                                       String message) {
        ByteBuf body = alloc.buffer(64);
        body.writeByte('S'); writeNullString(body, severity);
        body.writeByte('C'); writeNullString(body, sqlState);
        body.writeByte('M'); writeNullString(body, message);
        body.writeByte(0);   // terminator

        ByteBuf buf = alloc.buffer(body.readableBytes() + 5);
        buf.writeByte('E');
        buf.writeInt(body.readableBytes() + 4);
        buf.writeBytes(body);
        body.release();

        buf.writeBytes(readyForQuery(alloc));
        return buf;
    }

    // ── Private helpers ───────────────────────────────────────────────────────

    private static void writeCommandComplete(ByteBuf dest,
                                              ByteBufAllocator alloc,
                                              String tag) {
        byte[] tagBytes = (tag + '\0').getBytes(StandardCharsets.UTF_8);
        dest.writeByte('C');
        dest.writeInt(tagBytes.length + 4);
        dest.writeBytes(tagBytes);
    }

    private static void writeNullString(ByteBuf buf, String s) {
        buf.writeBytes(s.getBytes(StandardCharsets.UTF_8));
        buf.writeByte(0); // null terminator
    }
}
