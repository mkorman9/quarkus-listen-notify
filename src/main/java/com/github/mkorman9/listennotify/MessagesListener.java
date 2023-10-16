package com.github.mkorman9.listennotify;

import io.quarkus.vertx.ConsumeEvent;
import jakarta.enterprise.context.ApplicationScoped;
import lombok.extern.slf4j.Slf4j;

@ApplicationScoped
@Slf4j
public class MessagesListener {
    @ConsumeEvent(Message.EVENT_BUS_ADDRESS)
    public void onMessage(Message message) {
        log.info("Received: {}", message.payload());
    }
}
