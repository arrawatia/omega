package io.omega.proxy;

import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.types.Struct;
import org.apache.kafka.common.requests.ApiVersionsRequest;
import org.apache.kafka.common.requests.ResponseHeader;
import org.apache.kafka.common.requests.ResponseSend;

import io.omega.client.KafkaProtocolClient;
import io.omega.server.Request;
import io.omega.server.RequestChannel;
import io.omega.server.Response;

public class ApiVersionsHandler implements KafkaApiHandler {
    KafkaRequestDispatcher dispatcher;
    MetadataCache metadataCache;

    public ApiVersionsHandler(KafkaRequestDispatcher dispatcher, MetadataCache metadataCache) {
        this.dispatcher = dispatcher;
        this.metadataCache = metadataCache;
        dispatcher.registerHandler(ApiKeys.API_VERSIONS, this);
    }

    @Override
    public void handle(Request req, RequestChannel requestChannel, KafkaProtocolClient client) {
        ApiVersionsRequest request = (ApiVersionsRequest) req.body();
//            System.out.println("-----API_KEY-----" + ApiKeys.forId(req.header().apiKey()) +
//                    "-----request-----" + request.toString());
        int timeOutInMs = 1000;
        Struct responseBody = client.sendAnyNode(ApiKeys.API_VERSIONS, req.header().apiVersion(), request, timeOutInMs);
//                System.out.println("-----responseBody-----" + responseBody);
        ResponseHeader responseHeader = new ResponseHeader(req.header().correlationId());
        requestChannel.sendResponse(new Response(req, new ResponseSend(req.connectionId(), responseHeader, responseBody)));
    }
}
