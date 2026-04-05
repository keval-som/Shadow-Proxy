package com.shadow.proxy.protocol.postgres;

import java.nio.charset.StandardCharsets;
import java.util.List;

import com.shadow.proxy.protocol.CommandType;
import com.shadow.proxy.state.SessionManager;
import com.shadow.proxy.state.SessionState;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ByteToMessageDecoder;
import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.delete.Delete;
import net.sf.jsqlparser.statement.insert.Insert;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.statement.update.Update;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PostgresCommandDecoder extends ByteToMessageDecoder {

    private static final Logger log = LoggerFactory.getLogger(PostgresCommandDecoder.class);
    private static final int SSL_REQUEST_CODE = 80877103; // 0x04D2162F

    private final SessionManager sessionManager;
    private boolean startupComplete = false;
    private String  sessionId       = null;

    public PostgresCommandDecoder(SessionManager sessionManager) {
        this.sessionManager = sessionManager;
    }

    private void handleStartup(ChannelHandlerContext ctx, ByteBuf in, List<Object> out) {
        // Both SSLRequest and StartupMessage start with a 4-byte length followed by
        // a 4-byte code. Read both before deciding how to respond.
        if (in.readableBytes() < 8) return;

        int length = in.readInt();
        int code   = in.readInt();

        // SSLRequest magic: length=8, code=80877103 (0x04D2162F)
        if (length == 8 && code == SSL_REQUEST_CODE) {
            // Tell the client we don't support SSL — single byte 'N'
            ByteBuf response = ctx.alloc().buffer(1);
            response.writeByte('N');
            ctx.writeAndFlush(response);
            // Leave startupComplete = false so the next packet (real startup) is handled
            return;
        }

        // Real StartupMessage: skip remaining parameter bytes (length - 8 already read)
        if (length > 8) {
            in.skipBytes(length - 8);
        }
        startupComplete = true;
        sessionId = ctx.channel().id().asLongText();

        SessionState session = SessionState.authenticated(sessionId, "client", "shadowdb");
        sessionManager.save(session).whenComplete((v, ex) -> {
            if (ex != null) log.warn("Session save failed: {}", ex.getMessage());
        });

        // AuthenticationOk ('R') + ReadyForQuery ('Z')
        ByteBuf response = ctx.alloc().buffer(13);
        response.writeByte('R');
        response.writeInt(8);
        response.writeInt(0);
        response.writeByte('Z');
        response.writeInt(5);
        response.writeByte('I');
        ctx.writeAndFlush(response);
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
        if (sessionId != null) {
            sessionManager.delete(sessionId);
        }
        super.channelInactive(ctx);
    }

    @Override
    protected void decode(ChannelHandlerContext ctx, ByteBuf in, List<Object> out) {
        // Phase 1: startup (no type byte)
        if (!startupComplete) {
            if (in.readableBytes() < 4)
                return; // wait for more data
            // read length, read protocol version, send AuthenticationOk + ReadyForQuery
            handleStartup(ctx, in, out);
            return;
        }

        // Phase 2: normal messages
        if (in.readableBytes() < 5)
            return; // need type + length
        in.markReaderIndex();

        byte messageType = in.readByte();
        int length = in.readInt() - 4; // length includes itself

        if (in.readableBytes() < length) {
            in.resetReaderIndex(); // not enough data yet, wait
            return;
        }

        switch ((char) messageType) {
            case 'Q' -> handleSimpleQuery(in, length, out); // simple query
            case 'X' -> ctx.close(); // terminate
            default -> in.skipBytes(length); // ignore others for now
        }
    }

    private void handleSimpleQuery(ByteBuf in, int length, List<Object> out) {
        // The payload is a null-terminated string
        String sql = in.readCharSequence(length - 1, StandardCharsets.UTF_8).toString();
        in.skipBytes(1); // null terminator

        CommandType type = classify(sql);
        out.add(PostgresCommand.of(type, sql, sessionId != null ? sessionId : "unknown"));
    }

    private CommandType classify(String sql) {
        try {
            Statement stmt = CCJSqlParserUtil.parse(sql);
            if (stmt instanceof Select)
                return CommandType.SELECT;
            if (stmt instanceof Insert)
                return CommandType.INSERT;
            if (stmt instanceof Update)
                return CommandType.UPDATE;
            if (stmt instanceof Delete)
                return CommandType.DELETE;
            return CommandType.DDL;
        } catch (JSQLParserException e) {
            // JSQLParser can't parse CONTROL statements (BEGIN, COMMIT, ROLLBACK)
            return classifyByPrefix(sql);
        }
    }

    private CommandType classifyByPrefix(String sql) {
        String upper = sql.stripLeading().toUpperCase();
        if (upper.startsWith("SELECT"))
            return CommandType.SELECT;
        if (upper.startsWith("INSERT"))
            return CommandType.INSERT;
        if (upper.startsWith("UPDATE"))
            return CommandType.UPDATE;
        if (upper.startsWith("DELETE"))
            return CommandType.DELETE;
        if (upper.startsWith("BEGIN")
                || upper.startsWith("COMMIT")
                || upper.startsWith("ROLLBACK")
                || upper.startsWith("SAVEPOINT")
                || upper.startsWith("RELEASE"))
            return CommandType.CONTROL;
        if (upper.startsWith("CREATE")
                || upper.startsWith("DROP")
                || upper.startsWith("ALTER")
                || upper.startsWith("TRUNCATE")
                || upper.startsWith("RENAME"))
            return CommandType.DDL;
        // Unknown or unparseable — treat as CONTROL (no cache invalidation)
        log.warn("Unrecognised SQL prefix, treating as CONTROL to avoid cache wipe: [{}]",
                upper.length() > 60 ? upper.substring(0, 60) + "..." : upper);
        return CommandType.CONTROL;
    }
}
