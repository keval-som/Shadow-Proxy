package com.shadow.proxy.protocol;

public class ProtocolException extends Exception {

    private final String sqlState; // 5-char PostgreSQL SQLSTATE code e.g. "08P01"

    public ProtocolException(String message, String sqlState) {
        super(message);
        this.sqlState = sqlState;
    }

    public ProtocolException(String message, String sqlState, Throwable cause) {
        super(message, cause);
        this.sqlState = sqlState;
    }

    public String getSqlState() {
        return sqlState;
    }
}
