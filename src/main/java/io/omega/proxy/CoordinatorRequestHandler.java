package io.omega.proxy;

import org.apache.kafka.common.Node;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.types.Struct;
import org.apache.kafka.common.requests.GroupCoordinatorRequest;
import org.apache.kafka.common.requests.GroupCoordinatorResponse;
import org.apache.kafka.common.requests.ResponseHeader;
import org.apache.kafka.common.requests.ResponseSend;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.omega.client.KafkaProtocolClient;
import io.omega.server.Request;
import io.omega.server.RequestChannel;
import io.omega.server.Response;

public class CoordinatorRequestHandler implements KafkaApiHandler {

    private static final Logger log = LoggerFactory.getLogger(GroupCoordinatorHandler.class);

    KafkaRequestDispatcher dispatcher;
    MetadataCache metadataCache;

    public CoordinatorRequestHandler(KafkaRequestDispatcher dispatcher, MetadataCache metadataCache) {
        this.dispatcher = dispatcher;
        this.metadataCache = metadataCache;
        dispatcher.registerHandler(ApiKeys.JOIN_GROUP, this);
        dispatcher.registerHandler(ApiKeys.LEAVE_GROUP, this);
        dispatcher.registerHandler(ApiKeys.HEARTBEAT, this);
        dispatcher.registerHandler(ApiKeys.SYNC_GROUP, this);
        dispatcher.registerHandler(ApiKeys.OFFSET_COMMIT, this);
        dispatcher.registerHandler(ApiKeys.OFFSET_FETCH, this);
    }

    @Override
    public void handle(Request req, RequestChannel requestChannel, KafkaProtocolClient client) {
        String groupId = (String) req.body().toStruct().get("group_id");
        Struct responseBody = client.sendSync(metadataCache.coordinator(groupId), ApiKeys.forId(req.header().apiKey()), req.header().apiVersion(), req.body(), 1000000);

        ResponseHeader responseHeader = new ResponseHeader(req.header().correlationId());
        requestChannel.sendResponse(new Response(req, new ResponseSend(req.connectionId(), responseHeader, responseBody)));
    }
}
