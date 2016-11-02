package io.omega.proxy;

import sun.reflect.generics.reflectiveObjects.NotImplementedException;

import io.omega.client.KafkaProtocolClient;
import io.omega.server.Request;
import io.omega.server.RequestChannel;

/**
 * Created by sumit on 11/1/16.
 */
public interface KafkaApiHandler {

    void handle(Request req, RequestChannel requestChannel, KafkaProtocolClient client);

}
