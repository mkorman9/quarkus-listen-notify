package com.github.mkorman9.listennotify;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.quarkus.runtime.StartupEvent;
import io.quarkus.scheduler.Scheduled;
import io.smallrye.common.annotation.RunOnVirtualThread;
import io.vertx.core.eventbus.EventBus;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.event.Observes;
import jakarta.inject.Inject;
import lombok.extern.slf4j.Slf4j;

import java.sql.SQLException;

@ApplicationScoped
@Slf4j
public class ReceivingJob {
    private static final int RECEIVE_TIMEOUT_MS = 250;

    @Inject
    ConnectionHolder connectionHolder;

    @Inject
    EventBus eventBus;

    @Inject
    ObjectMapper objectMapper;

    public void onStart(@Observes StartupEvent startupEvent) {
        connectionHolder.initialize();
    }

    @Scheduled(every = "1s")
    @RunOnVirtualThread
    public void onReceive() {
        connectionHolder.acquire(connection -> {
            try {
                var notifications = connection.getNotifications(RECEIVE_TIMEOUT_MS);
                if (notifications == null) {
                    return;
                }

                for (var notification : notifications) {
                    var channel = Channel.fromChannelName(notification.getName());
                    if (channel == null) {
                        log.error(
                            "Received notification for unregistered channel: {}, ({})",
                            notification.getName(),
                            notification.getParameter()
                        );
                        continue;
                    }

                    try {
                        var message = objectMapper.readValue(notification.getParameter(), channel.getPayloadClass());
                        eventBus.send(channel.getEventBusAddress(), message);
                    } catch (JsonProcessingException e) {
                        log.error(
                            "Notification deserialization error from channel {} ({})",
                            notification.getName(),
                            notification.getParameter(),
                            e
                        );
                    }
                }
            } catch (SQLException e) {
                log.error("Error while fetching notifications from the database", e);
                throw new RuntimeException(e);
            }
        });
    }
}
