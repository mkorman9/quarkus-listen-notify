package com.github.mkorman9.listennotify.notifications;

import com.github.mkorman9.listennotify.Message;
import lombok.Getter;

@Getter
public enum Channel {
    MESSAGES(Message.EVENT_BUS_ADDRESS, Message.class);

    private final String eventBusAddress;
    private final Class<?> payloadClass;

    Channel(String eventBusAddress, Class<?> payloadClass) {
        this.eventBusAddress = eventBusAddress;
        this.payloadClass = payloadClass;
    }

    public String channelName() {
        return name().toLowerCase();
    }

    public static Channel fromChannelName(String channelName) {
        try {
            return valueOf(channelName.toUpperCase());
        } catch (IllegalArgumentException e) {
            return null;
        }
    }
}
