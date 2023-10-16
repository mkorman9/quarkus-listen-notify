package com.github.mkorman9.listennotify;

import io.quarkus.runtime.StartupEvent;
import io.quarkus.scheduler.Scheduled;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.event.Observes;
import jakarta.inject.Inject;
import lombok.extern.slf4j.Slf4j;

import java.time.Instant;
import java.util.concurrent.atomic.AtomicBoolean;

@ApplicationScoped
@Slf4j
public class SendingJob {
    @Inject
    Sender sender;

    private final AtomicBoolean started = new AtomicBoolean(false);

    public void onStart(@Observes StartupEvent startupEvent) {
        started.set(true);
    }

    @Scheduled(every = "1s")
    public void onSend() {
        if (!started.get()) {
            return;
        }

        sender.send(Channel.MESSAGES, new Message(Instant.now().toString()));
    }
}
