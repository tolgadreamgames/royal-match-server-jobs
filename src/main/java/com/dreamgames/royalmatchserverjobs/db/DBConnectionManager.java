package com.dreamgames.royalmatchserverjobs.db;

import com.dreamgames.royalmatchserverjobs.util.ServerProperties;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;

import java.io.FileInputStream;
import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Properties;

public class DBConnectionManager {
    private static HikariConfig masterHikariConfig = new HikariConfig();
    private static HikariDataSource masterDataSource;

    private static HikariConfig readonlyHikariConfig = new HikariConfig();
    private static HikariDataSource readonlyDataSource;

    public static void init() {
        masterHikariConfig.setJdbcUrl(ServerProperties.getString("db.master.url"));
        masterHikariConfig.setUsername(ServerProperties.getString("db.master.username"));
        masterHikariConfig.setPassword(ServerProperties.getString("db.master.password"));
        masterHikariConfig.setAutoCommit(ServerProperties.getBoolean("hikari.auto-commit"));
        masterHikariConfig.setConnectionTimeout(ServerProperties.getLong("hikari.connection-timeout"));
        masterHikariConfig.setMinimumIdle(ServerProperties.getInt("hikari.minimum-idle"));
        masterHikariConfig.setMaximumPoolSize(ServerProperties.getInt("hikari.maximum-pool-size"));
        masterDataSource = new HikariDataSource(masterHikariConfig);

        readonlyHikariConfig.setJdbcUrl(ServerProperties.getString("db.readonly.url"));
        readonlyHikariConfig.setUsername(ServerProperties.getString("db.readonly.username"));
        readonlyHikariConfig.setPassword(ServerProperties.getString("db.readonly.password"));
        readonlyHikariConfig.setAutoCommit(ServerProperties.getBoolean("hikari.auto-commit"));
        readonlyHikariConfig.setConnectionTimeout(ServerProperties.getLong("hikari.connection-timeout"));
        readonlyHikariConfig.setMinimumIdle(ServerProperties.getInt("hikari.minimum-idle"));
        readonlyHikariConfig.setMaximumPoolSize(ServerProperties.getInt("hikari.maximum-pool-size"));
        readonlyDataSource = new HikariDataSource(readonlyHikariConfig);
    }

    public static Connection getMasterConnection() throws SQLException {
        return masterDataSource.getConnection();
    }

    public static Connection getReadonlyConnection() throws SQLException {
        return readonlyDataSource.getConnection();
    }
}
