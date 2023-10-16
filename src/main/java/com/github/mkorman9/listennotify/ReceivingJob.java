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

    private final AtomicBoolean isSubscribed = new AtomicBoolean(false);
    private Connection connection;
    private PgConnection pgConnection;
    private int fetchErrorsCount;
    private boolean shouldReconnect;

    @Inject
    DataSource dataSource;

    @Inject
    EventBus eventBus;

    @Inject
    ObjectMapper objectMapper;

    public void onStart(@Observes StartupEvent startupEvent) {
        acquireConnection();
    }

    @Scheduled(every = "1s")
    public void onReceive() {
        if (!isSubscribed.get()) {
            if (shouldReconnect) {
                acquireConnection();
            } else {
                return;
            }
        }

        try {
            var notifications = pgConnection.getNotifications(RECEIVE_TIMEOUT_MS);
            fetchErrorsCount = 0;

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
            fetchErrorsCount++;

            try {
                if (fetchErrorsCount > CONNECTION_ERRORS_THRESHOLD) {
                    isSubscribed.set(false);
                    shouldReconnect = true;

                    connection.close();
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

            fetchErrorsCount = 0;
            isSubscribed.set(true);
            shouldReconnect = false;
        } catch (SQLException e) {
            log.error("Error while acquiring database connection", e);
            shouldReconnect = true;

            throw new RuntimeException(e);
        }
    }
}
