package com.github.mkorman9.listennotify.notifications;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.quarkus.runtime.ShutdownEvent;
import io.quarkus.runtime.StartupEvent;
import io.quarkus.scheduler.Scheduled;
import io.smallrye.common.annotation.RunOnVirtualThread;
import io.vertx.core.eventbus.EventBus;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.event.Observes;
import jakarta.inject.Inject;
import lombok.extern.slf4j.Slf4j;
import org.postgresql.PGNotification;

import javax.sql.DataSource;
import java.sql.SQLException;
import java.util.concurrent.atomic.AtomicBoolean;

@ApplicationScoped
@Slf4j
public class NotificationReceivingJob {
    private static final int RECEIVE_TIMEOUT_MS = 250;

    private ConnectionHolder connectionHolder;
    private final AtomicBoolean isStarted = new AtomicBoolean(false);

    @Inject
    DataSource dataSource;

    @Inject
    EventBus eventBus;

    @Inject
    ObjectMapper objectMapper;

    public void onStart(@Observes StartupEvent startupEvent) {
        connectionHolder = new ConnectionHolder(dataSource);
        isStarted.set(true);
    }

    public void onShutdown(@Observes ShutdownEvent shutdownEvent) {
        isStarted.set(false);
    }

    @Scheduled(every = "1s", concurrentExecution = Scheduled.ConcurrentExecution.SKIP)
    @RunOnVirtualThread
    public void onReceive() {
        if (!isStarted.get()) {
            return;
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
