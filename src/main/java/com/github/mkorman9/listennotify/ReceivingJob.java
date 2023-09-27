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
import org.postgresql.PGConnection;

import javax.sql.DataSource;
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

    @Inject
    DataSource dataSource;

    @Inject
    EventBus eventBus;

    @Inject
    ObjectMapper objectMapper;

    private final AtomicBoolean started = new AtomicBoolean(false);
    private PGConnection pgConnection;

    public void onStart(@Observes StartupEvent startupEvent) {
        try {
            var connection = dataSource.getConnection();

            for (var channel : CHANNELS.keySet()) {
                var statement = connection.createStatement();
                statement.execute("LISTEN " + channel);
                statement.close();
            }

            pgConnection = connection.unwrap(PGConnection.class);
            started.set(true);
        } catch (SQLException e) {
            log.error("SQL Error", e);
        }
    }

    @Scheduled(every = "1s")
    public void onReceive() {
        if (!started.get()) {
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
            log.error("SQL Error", e);
        }
    }
}
