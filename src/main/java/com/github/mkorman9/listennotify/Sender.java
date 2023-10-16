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
        try (
            var connection = dataSource.getConnection();
            var statement = connection.createStatement()
        ) {
            statement.execute(
                String.format(
                    "NOTIFY %s, '%s'",
                    channel.channelName(),
                    objectMapper.writeValueAsString(message)
                )
            );
        } catch (SQLException e) {
            log.error("Error while sending database notification to channel {}", channel.channelName(), e);
        } catch (JsonProcessingException e) {
            log.error("Notification serialization error to channel {}", channel.channelName(), e);
        }
    }
}
