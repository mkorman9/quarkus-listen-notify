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

import javax.sql.DataSource;
import java.sql.SQLException;
import java.util.Map;

@ApplicationScoped
@Slf4j
public class ReceivingJob {
    private static final Map<String, Class<?>> CHANNELS = Map.of(
        "messages", Message.class
    );
    private static final int RECEIVE_TIMEOUT_MS = 250;

    private ConnectionHolder connectionHolder;

    @Inject
    DataSource dataSource;

    @Inject
    EventBus eventBus;

    @Inject
    ObjectMapper objectMapper;

    public void onStart(@Observes StartupEvent startupEvent) {
        connectionHolder = new ConnectionHolder(dataSource, CHANNELS);
        connectionHolder.initialize();
    }

    @Scheduled(every = "1s")
    public void onReceive() {
        var maybePgConnection = connectionHolder.getConnection();
        if (maybePgConnection.isEmpty()) {
            return;
        }

        var pgConnection = maybePgConnection.get();

        try {
            var notifications = pgConnection.getNotifications(RECEIVE_TIMEOUT_MS);
            connectionHolder.resetSqlErrors();

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
            log.error("Error while fetching notifications from the database", e);
            connectionHolder.reportSqlError();
        }
    }
}
