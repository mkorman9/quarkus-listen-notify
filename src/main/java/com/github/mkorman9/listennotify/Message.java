package com.github.mkorman9.listennotify;

public record Message(
    String payload
) {
    public static final String EVENT_BUS_ADDRESS = "messages";
}
