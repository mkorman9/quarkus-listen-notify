package com.github.mkorman9.listennotify;

import io.quarkus.vertx.ConsumeEvent;
import jakarta.enterprise.context.ApplicationScoped;
import lombok.extern.slf4j.Slf4j;

@ApplicationScoped
@Slf4j
public class MessagesListener {
    @ConsumeEvent("messages")
    public void onMessage(Message message) {
        log.info("Received: {}", message.payload());
    }
}
