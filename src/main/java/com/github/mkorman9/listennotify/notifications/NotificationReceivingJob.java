package com.github.mkorman9.listennotify.notifications;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.quarkus.scheduler.Scheduled;
import io.smallrye.common.annotation.RunOnVirtualThread;
import io.vertx.core.eventbus.EventBus;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import lombok.extern.slf4j.Slf4j;
import org.postgresql.PGNotification;

import javax.sql.DataSource;
import java.sql.SQLException;

@ApplicationScoped
@Slf4j
public class NotificationReceivingJob {
    private static final int RECEIVE_TIMEOUT_MS = 250;

    private ConnectionHolder connectionHolder;

    @Inject
    DataSource dataSource;

    @Inject
    EventBus eventBus;

    @Inject
    ObjectMapper objectMapper;

    @Scheduled(every = "1s", concurrentExecution = Scheduled.ConcurrentExecution.SKIP)
    @RunOnVirtualThread
    public void onReceive() {
        if (connectionHolder == null) {
            connectionHolder = new ConnectionHolder(dataSource);
        }

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

                    routeMessage(channel, notification);
                }
            } catch (SQLException e) {
                log.error("Error while fetching notifications from the database", e);
                throw new RuntimeException(e);
            }
        });
    }

    private void routeMessage(Channel channel, PGNotification notification) {
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
}
