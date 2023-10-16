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
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

@ApplicationScoped
@Slf4j
public class ConnectionHolder {
    private static final int SQL_ERRORS_THRESHOLD = 5;

    private final AtomicReference<ConnectionState> connectionState = new AtomicReference<>(
        ConnectionState.builder()
            .active(false)
            .build()
    );
    private int executionErrorsCount;

    @Inject
    DataSource dataSource;

    public void initialize() {
        connectionState.set(reconnect());
    }

    public void acquire(Consumer<PgConnection> consumer) {
        try {
            getConnection().ifPresent(consumer);
            resetExecutionErrorsCounter();
        } catch (Exception e) {
            reportExecutionError();
        }
    }

    private Optional<PgConnection> getConnection() {
        var cachedConnection = connectionState.get();
        if (!cachedConnection.active()) {
            if (!cachedConnection.shouldReconnect()) {
                return Optional.empty();
            }

            cachedConnection = reconnect();
            connectionState.set(cachedConnection);
        }

        return Optional.ofNullable(cachedConnection.active() ? cachedConnection.pgConnection() : null);
    }

    private void reportExecutionError() {
        executionErrorsCount++;

        if (executionErrorsCount > SQL_ERRORS_THRESHOLD) {
            var cachedConnection = connectionState.getAndSet(ConnectionState.builder()
                .active(false)
                .shouldReconnect(false)
                .build()
            );
            if (cachedConnection.connection != null) {
                try {
                    cachedConnection.connection.close();
                } catch (SQLException e) {
                    // ignore
                }
            }

            resetExecutionErrorsCounter();
            connectionState.set(reconnect());
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

    @Builder(toBuilder = true)
    private record ConnectionState(
        boolean active,
        boolean shouldReconnect,
        Connection connection,
        PgConnection pgConnection
    ) {
    }
}
