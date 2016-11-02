package io.omega.proxy;

import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.types.Struct;
import org.apache.kafka.common.requests.FetchRequest;
import org.apache.kafka.common.requests.ResponseHeader;
import org.apache.kafka.common.requests.ResponseSend;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

import io.omega.client.KafkaProtocolClient;
import io.omega.server.Request;
import io.omega.server.RequestChannel;
import io.omega.server.Response;

/**
 * Created by sumit on 11/1/16.
 */
public class FetchHandler implements KafkaApiHandler {
    private static final Logger log = LoggerFactory.getLogger(FetchHandler.class);

    KafkaRequestDispatcher dispatcher;
    MetadataCache metadataCache;

    public FetchHandler(KafkaRequestDispatcher dispatcher, MetadataCache metadataCache) {
        this.dispatcher = dispatcher;
        this.metadataCache = metadataCache;
        dispatcher.registerHandler(ApiKeys.FETCH, this);
    }


    @Override
    public void handle(Request req, RequestChannel requestChannel, KafkaProtocolClient client) {
        FetchRequest request = (FetchRequest) req.body();
        Map<TopicPartition, FetchRequest.PartitionData> p = request.fetchData();
        TopicPartition topicPartition = p.keySet().stream().findFirst().get();
        Node target = metadataCache.getLeaderForTopicPartition(topicPartition);
        Struct response = client.sendSync(target, ApiKeys.FETCH, req.header().apiVersion(), request, 1000000000);
        log.trace("FETCH response = {}", response);
        ResponseHeader responseHeader = new ResponseHeader(req.header().correlationId());
        requestChannel.sendResponse(new Response(req, new ResponseSend(req.connectionId(), responseHeader, response)));
    }
}
