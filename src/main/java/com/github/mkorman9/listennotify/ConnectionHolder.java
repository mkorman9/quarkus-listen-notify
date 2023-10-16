package com.github.mkorman9.listennotify;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import lombok.Builder;
import lombok.extern.slf4j.Slf4j;
import org.postgresql.jdbc.PgConnection;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

@ApplicationScoped
@Slf4j
public class ConnectionHolder {
    private static final int ERRORS_THRESHOLD_BEFORE_RECONNECT = 5;
    private static final int CONNECTION_ACQUIRE_BACKOFF_BASE = 2;
    private static final int CONNECTION_ACQUIRE_MAX_TIME_SEC = 64;

    private final AtomicBoolean acquired = new AtomicBoolean(true);
    private ConnectionState connectionState = ConnectionState.builder()
        .active(false)
        .build();
    private int executionErrorsCount;

    @Inject
    DataSource dataSource;

    public void initialize() {
        connectionState = reconnect();
        acquired.set(false);
    }

    public void acquire(Consumer<PgConnection> consumer) {
        if (acquired.getAndSet(true)) {
            return;
        }

        try {
            consumer.accept(getConnection());
            resetExecutionErrorsCounter();
        } catch (Exception e) {
            reportExecutionError();
        } finally {
            acquired.set(false);
        }
    }

    private PgConnection getConnection() {
        if (!connectionState.active()) {
            waitForConnection();
        }

        return connectionState.pgConnection();
    }

    private void waitForConnection() {
        var i = 0;
        while (true) {
            connectionState = reconnect();
            if (connectionState.active()) {
                return;
            }

            var backoffTime = ((long)
                Math.min(Math.pow(CONNECTION_ACQUIRE_BACKOFF_BASE, i), CONNECTION_ACQUIRE_MAX_TIME_SEC)
            );
            log.error("Trying to acquire database connection (try #{}), waiting {}s", i, backoffTime);

            try {
                Thread.sleep(backoffTime * 1000);
            } catch (InterruptedException e) {
                // ignored
            }

            i++;
        }
    }

    private void reportExecutionError() {
        executionErrorsCount++;

        if (executionErrorsCount > ERRORS_THRESHOLD_BEFORE_RECONNECT) {
            var connection = connectionState.connection;
            if (connection != null) {
                try {
                    connection.close();
                } catch (SQLException e) {
                    // ignore
                }
            }

            connectionState = ConnectionState.builder()
                .active(false)
                .build();

            resetExecutionErrorsCounter();
        }
    }

    private void resetExecutionErrorsCounter() {
        executionErrorsCount = 0;
    }

    private ConnectionState reconnect() {
        Connection connection = null;
        try {
            connection = dataSource.getConnection();

            for (var channel : Channel.values()) {
                try (var statement = connection.createStatement()) {
                    statement.execute("LISTEN " + channel.channelName());
                }
            }

            return ConnectionState.builder()
                .active(true)
                .connection(connection)
                .pgConnection(connection.unwrap(PgConnection.class))
                .build();
        } catch (SQLException e) {
            if (connection != null) {
                try {
                    connection.close();
                } catch (SQLException ex) {
                    // ignore
                }
            }

            log.error("Error while acquiring database connection", e);

            return ConnectionState.builder()
                .active(false)
                .build();
        }
    }

    @Builder
    private record ConnectionState(
        boolean active,
        Connection connection,
        PgConnection pgConnection
    ) {
    }
}
