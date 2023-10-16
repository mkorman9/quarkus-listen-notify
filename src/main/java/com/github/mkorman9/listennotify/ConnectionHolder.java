package com.github.mkorman9.listennotify;

import lombok.extern.slf4j.Slf4j;
import org.postgresql.jdbc.PgConnection;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;

@Slf4j
public class ConnectionHolder {
    private static final int SQL_ERRORS_THRESHOLD = 5;

    private final DataSource dataSource;
    private final Map<String, Class<?>> channelsMapping;
    private final AtomicBoolean isActive = new AtomicBoolean(false);
    private Connection connection;
    private PgConnection pgConnection;
    private int sqlErrorsCount;
    private boolean shouldReconnect;

    public ConnectionHolder(DataSource dataSource, Map<String, Class<?>> channelsMapping) {
        this.dataSource = dataSource;
        this.channelsMapping = channelsMapping;
    }

    public void initialize() {
        reconnect();
    }

    public Optional<PgConnection> getConnection() {
        if (!isActive.get()) {
            if (shouldReconnect) {
                reconnect();

                if (isActive.get()) {
                    return Optional.of(pgConnection);
                } else {
                    return Optional.empty();
                }
            } else {
                return Optional.empty();
            }
        }

        return Optional.of(pgConnection);
    }

    public void reportSqlError() {
        sqlErrorsCount++;

        if (sqlErrorsCount > SQL_ERRORS_THRESHOLD) {
            try {
                isActive.set(false);
                sqlErrorsCount = 0;

                connection.close();
                reconnect();
            } catch (SQLException ex) {
                // ignore
            }
        }
    }

    public void resetSqlErrors() {
        sqlErrorsCount = 0;
    }

    private void reconnect() {
        try {
            connection = dataSource.getConnection();

            for (var channel : channelsMapping.keySet()) {
                var statement = connection.createStatement();
                statement.execute("LISTEN " + channel);
                statement.close();
            }

            pgConnection = connection.unwrap(PgConnection.class);

            isActive.set(true);
            shouldReconnect = false;
        } catch (SQLException e) {
            if (connection != null) {
                try {
                    connection.close();
                } catch (SQLException ex) {
                    // ignore
                }
            }

            log.error("Error while acquiring database connection", e);
            shouldReconnect = true;
        }
    }
}
