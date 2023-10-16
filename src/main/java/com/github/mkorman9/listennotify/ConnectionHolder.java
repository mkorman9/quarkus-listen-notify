package com.github.mkorman9.listennotify;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
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

    private final AtomicBoolean isActive = new AtomicBoolean(false);
    private final AtomicBoolean shouldReconnect = new AtomicBoolean(false);
    private Connection connection;
    private PgConnection pgConnection;
    private int executionErrorsCount;

    @Inject
    DataSource dataSource;

    public void initialize() {
        reconnect();
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
        if (!isActive.get()) {
            if (shouldReconnect.get()) {
                return reconnect();
            }

            return Optional.empty();
        }

        return Optional.of(pgConnection);
    }

    private void reportExecutionError() {
        executionErrorsCount++;

        if (executionErrorsCount > SQL_ERRORS_THRESHOLD) {
            try {
                isActive.set(false);
                resetExecutionErrorsCounter();

                connection.close();
                reconnect();
            } catch (SQLException ex) {
                // ignore
            }
        }
    }

    private void resetExecutionErrorsCounter() {
        executionErrorsCount = 0;
    }

    private Optional<PgConnection> reconnect() {
        try {
            connection = dataSource.getConnection();

            for (var channel : Channel.values()) {
                try (var statement = connection.createStatement()) {
                    statement.execute("LISTEN " + channel.channelName());
                }
            }

            pgConnection = connection.unwrap(PgConnection.class);
            isActive.set(true);
            shouldReconnect.set(false);

            return Optional.of(pgConnection);
        } catch (SQLException e) {
            if (connection != null) {
                try {
                    connection.close();
                } catch (SQLException ex) {
                    // ignore
                }
            }

            log.error("Error while acquiring database connection", e);
            shouldReconnect.set(true);

            return Optional.empty();
        }
    }
}
