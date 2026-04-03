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

public class PostgresCommandDecoder extends ByteToMessageDecoder {

    private final SessionManager sessionManager;
    private boolean startupComplete = false;
    private String  sessionId       = null;

    public PostgresCommandDecoder(SessionManager sessionManager) {
        this.sessionManager = sessionManager;
    }

    private void handleStartup(ChannelHandlerContext ctx, ByteBuf in, List<Object> out) {
        int length = in.readInt();
        in.skipBytes(length - 4);
        startupComplete = true;

        // Derive a stable session ID from the channel's unique ID
        sessionId = ctx.channel().id().asLongText();

        // Create and persist the session asynchronously
        SessionState session = SessionState.authenticated(sessionId, "client", "shadowdb");
        sessionManager.save(session).whenComplete((v, ex) -> {
            if (ex != null) {
                // Non-fatal: proxy continues even if Redis is temporarily down
                ctx.channel().attr(io.netty.util.AttributeKey.valueOf("sessionId"))
                        .set(sessionId);
            }
        });

        // Respond immediately: AuthenticationOk ('R') then ReadyForQuery ('Z')
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
                || upper.startsWith("ROLLBACK"))
            return CommandType.CONTROL;
        return CommandType.DDL;
    }
}
