package com.github.mkorman9.listennotify;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import lombok.extern.slf4j.Slf4j;

import javax.sql.DataSource;
import java.sql.SQLException;

@ApplicationScoped
@Slf4j
public class Sender {
    @Inject
    DataSource dataSource;

    @Inject
    ObjectMapper objectMapper;

    public void send(Channel channel, Object message) {
        var channelName = channel.name().toLowerCase();

        try (
            var connection = dataSource.getConnection();
            var statement = connection.createStatement()
        ) {
            statement.execute(String.format("NOTIFY %s, '%s'", channelName, objectMapper.writeValueAsString(message)));
        } catch (SQLException e) {
            log.error("SQL Error", e);
        } catch (JsonProcessingException e) {
            log.error("Serialization Error", e);
        }
    }
}
