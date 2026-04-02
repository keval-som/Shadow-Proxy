package com.shadow.proxy.backend;

import com.shadow.proxy.routing.BackendNode;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.SQLException;

/**
 * HikariCP connection pool scoped to a single BackendNode.
 * One instance per node (primary + each replica).
 */
public class BackendConnectionPool {

    private static final Logger log = LoggerFactory.getLogger(BackendConnectionPool.class);

    private final BackendNode     node;
    private final HikariDataSource dataSource;

    public BackendConnectionPool(BackendNode node,
                                  String dbName,
                                  String username,
                                  String password,
                                  int maxPoolSize) {
        this.node = node;

        HikariConfig config = new HikariConfig();
        config.setJdbcUrl(String.format("jdbc:postgresql://%s:%d/%s",
                node.host(), node.port(), dbName));
        config.setUsername(username);
        config.setPassword(password);
        config.setMaximumPoolSize(maxPoolSize);
        config.setMinimumIdle(2);
        config.setConnectionTimeout(3_000);
        config.setIdleTimeout(600_000);
        config.setMaxLifetime(1_800_000);
        config.setPoolName("pool-" + node.id());

        // Validate connections on borrow
        config.setConnectionTestQuery("SELECT 1");

        this.dataSource = new HikariDataSource(config);
        log.info("Connection pool created for node={} url={}", node.id(), config.getJdbcUrl());
    }

    /** Borrows a JDBC connection from the pool. Must be closed after use. */
    public Connection getConnection() throws SQLException {
        return dataSource.getConnection();
    }

    public BackendNode getNode() {
        return node;
    }

    public void close() {
        dataSource.close();
        log.info("Connection pool closed for node={}", node.id());
    }
}
