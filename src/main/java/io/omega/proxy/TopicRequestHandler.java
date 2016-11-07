package io.omega.proxy;

import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.types.Struct;
import org.apache.kafka.common.requests.ApiVersionsRequest;
import org.apache.kafka.common.requests.CreateTopicsRequest;
import org.apache.kafka.common.requests.ResponseHeader;
import org.apache.kafka.common.requests.ResponseSend;

import io.omega.client.KafkaProtocolClient;
import io.omega.server.Request;
import io.omega.server.RequestChannel;
import io.omega.server.Response;

public class TopicRequestHandler implements KafkaApiHandler {
    KafkaRequestDispatcher dispatcher;
    MetadataCache metadataCache;

    public TopicRequestHandler(KafkaRequestDispatcher dispatcher, MetadataCache metadataCache) {
        this.dispatcher = dispatcher;
        this.metadataCache = metadataCache;
        dispatcher.registerHandler(ApiKeys.CREATE_TOPICS, this);
        dispatcher.registerHandler(ApiKeys.DELETE_TOPICS, this);
    }

    @Override
    public void handle(Request req, RequestChannel requestChannel, KafkaProtocolClient client) {

//            System.out.println("-----API_KEY-----" + ApiKeys.forId(req.header().apiKey()) +
//                    "-----request-----" + request.toString());

        int timeOutInMs = 100000;
        Struct responseBody = client.sendSync(metadataCache.controller(), ApiKeys.forId(req.header().apiKey()), req.header().apiVersion(), req.body(), timeOutInMs);
//                System.out.println("-----responseBody-----" + responseBody);

        ResponseHeader responseHeader = new ResponseHeader(req.header().correlationId());

        requestChannel.sendResponse(new Response(req, new ResponseSend(req.connectionId(), responseHeader, responseBody)));
    }
}
