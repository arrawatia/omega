package io.omega.proxy;

import org.apache.kafka.common.protocol.ApiKeys;

import io.omega.server.Request;
import io.omega.server.RequestChannel;

public interface KafkaRequestDispatcher {
    void dispatch(Request req, RequestChannel requestChannel);
    void registerHandler(ApiKeys apiKey, KafkaApiHandler handler);
}
