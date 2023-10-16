package com.github.mkorman9.listennotify;

import lombok.Getter;

@Getter
public enum Channel {
    MESSAGES(Message.EVENT_BUS_ADDRESS, Message.class);

    private final String eventBusAddress;
    private final Class<?> clazz;

    Channel(String eventBusAddress, Class<?> clazz) {
        this.eventBusAddress = eventBusAddress;
        this.clazz = clazz;
    }
}
