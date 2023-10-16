package com.github.mkorman9.listennotify;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import lombok.Builder;
import lombok.extern.slf4j.Slf4j;
import org.postgresql.jdbc.PgConnection;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

@ApplicationScoped
@Slf4j
public class ConnectionHolder {
    private static final int SQL_ERRORS_THRESHOLD = 5;

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
            getConnection().ifPresent(consumer);
            resetExecutionErrorsCounter();
        } catch (Exception e) {
            reportExecutionError();
        } finally {
            acquired.set(false);
        }
    }

    private Optional<PgConnection> getConnection() {
        if (!connectionState.active()) {
            if (!connectionState.shouldReconnect()) {
                return Optional.empty();
            }

            connectionState = reconnect();
        }

        return Optional.ofNullable(connectionState.active() ? connectionState.pgConnection() : null);
    }

    private void reportExecutionError() {
        executionErrorsCount++;

        if (executionErrorsCount > SQL_ERRORS_THRESHOLD) {
            var connection = connectionState.connection;
            connectionState = ConnectionState.builder()
                .active(false)
                .shouldReconnect(false)
                .build();

            if (connection != null) {
                try {
                    connection.close();
                } catch (SQLException e) {
                    // ignore
                }
            }

            resetExecutionErrorsCounter();
            connectionState = reconnect();
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
                .shouldReconnect(false)
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
                .shouldReconnect(true)
                .build();
        }
    }

    @Builder
    private record ConnectionState(
        boolean active,
        boolean shouldReconnect,
        Connection connection,
        PgConnection pgConnection
    ) {
    }
}
