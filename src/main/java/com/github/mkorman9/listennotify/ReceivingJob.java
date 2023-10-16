package com.github.mkorman9.listennotify;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.quarkus.runtime.StartupEvent;
import io.quarkus.scheduler.Scheduled;
import io.vertx.core.eventbus.EventBus;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.event.Observes;
import jakarta.inject.Inject;
import lombok.extern.slf4j.Slf4j;
import org.postgresql.jdbc.PgConnection;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

@ApplicationScoped
@Slf4j
public class ReceivingJob {
    private static final Map<String, Class<?>> CHANNELS = Map.of(
        "messages", Message.class
    );
    private static final int RECEIVE_TIMEOUT_MS = 250;
    private static final int CONNECTION_ERRORS_THRESHOLD = 5;

    @Inject
    DataSource dataSource;

    @Inject
    EventBus eventBus;

    @Inject
    ObjectMapper objectMapper;

    private final AtomicBoolean connected = new AtomicBoolean(false);
    private Connection connection;
    private PgConnection pgConnection;
    private int connectionErrors;

    public void onStart(@Observes StartupEvent startupEvent) {
        acquireConnection();
    }

    @Scheduled(every = "1s")
    public void onReceive() {
        if (!connected.get()) {
            return;
        }

        try {
            var notifications = pgConnection.getNotifications(RECEIVE_TIMEOUT_MS);
            if (notifications == null) {
                return;
            }

            for (var notification : notifications) {
                var clazz = CHANNELS.get(notification.getName());
                if (clazz == null) {
                    log.error("Unregistered channel: {}", notification.getName());
                    continue;
                }

                try {
                    var message = objectMapper.readValue(notification.getParameter(), clazz);
                    eventBus.send(notification.getName(), message);
                } catch (JsonProcessingException e) {
                    log.error("Deserialization Error", e);
                }
            }
        } catch (SQLException e) {
            connectionErrors++;

            try {
                if (connectionErrors > CONNECTION_ERRORS_THRESHOLD) {
                    connected.set(false);
                    connection.close();
                    connectionErrors = 0;

                    acquireConnection();  // reconnect
                    return;
                }
            } catch (SQLException ex) {
                // ignore
            }

            log.error("Error while fetching notifications from the database", e);
        }
    }

    private void acquireConnection() {
        try {
            connection = dataSource.getConnection();

            for (var channel : CHANNELS.keySet()) {
                var statement = connection.createStatement();
                statement.execute("LISTEN " + channel);
                statement.close();
            }

            pgConnection = connection.unwrap(PgConnection.class);
            connected.set(true);
        } catch (SQLException e) {
            log.error("Error while acquiring database connection", e);
        }
    }
}
