package com.github.mkorman9.listennotify.notifications;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.postgresql.jdbc.PgConnection;

import javax.sql.DataSource;
import java.sql.SQLException;

@ApplicationScoped
public class NotificationSender {
    @Inject
    DataSource dataSource;

    @Inject
    ObjectMapper objectMapper;

    public void send(Channel channel, Object payload) throws NotificationSendException {
        try (
            var connection = dataSource.getConnection();
            var statement = connection.createStatement()
        ) {
            var pgConnection = connection.unwrap(PgConnection.class);

            statement.execute(
                String.format(
                    "NOTIFY %s, '%s'",
                    channel.channelName(),
                    pgConnection.escapeString(objectMapper.writeValueAsString(payload))
                )
            );
        } catch (SQLException e) {
            throw new NotificationSendException(
                "Error while sending database notification to channel " + channel.channelName(),
                e
            );
        } catch (JsonProcessingException e) {
            throw new NotificationSendException(
                "Error while serializing notification for channel " + channel.channelName(),
                e
            );
        }
    }
}
