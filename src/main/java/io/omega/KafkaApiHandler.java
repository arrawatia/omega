package io.omega;

import io.omega.server.Request;
import io.omega.server.RequestChannel;

public interface KafkaApiHandler {
    void handle(Request req, RequestChannel requestChannel);
}
