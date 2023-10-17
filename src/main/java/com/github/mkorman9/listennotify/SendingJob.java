package com.github.mkorman9.listennotify;

import com.github.mkorman9.listennotify.notifications.Channel;
import com.github.mkorman9.listennotify.notifications.NotificationSender;
import io.quarkus.scheduler.Scheduled;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import lombok.extern.slf4j.Slf4j;

import java.time.Instant;

@ApplicationScoped
@Slf4j
public class SendingJob {
    @Inject
    NotificationSender notificationSender;

    @Scheduled(every = "1s")
    public void onSend() {
        notificationSender.send(Channel.MESSAGES, new Message(Instant.now().toString()));
    }
}
