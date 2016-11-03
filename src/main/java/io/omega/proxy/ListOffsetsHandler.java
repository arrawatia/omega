package io.omega.proxy;

import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.types.Struct;
import org.apache.kafka.common.requests.FetchRequest;
import org.apache.kafka.common.requests.ListOffsetRequest;
import org.apache.kafka.common.requests.ResponseHeader;
import org.apache.kafka.common.requests.ResponseSend;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

import io.omega.client.KafkaProtocolClient;
import io.omega.server.Request;
import io.omega.server.RequestChannel;
import io.omega.server.Response;

public class ListOffsetsHandler implements KafkaApiHandler {
    private static final Logger log = LoggerFactory.getLogger(ListOffsetsHandler.class);

    KafkaRequestDispatcher dispatcher;
    MetadataCache metadataCache;

    public ListOffsetsHandler(KafkaRequestDispatcher dispatcher, MetadataCache metadataCache) {
        this.dispatcher = dispatcher;
        this.metadataCache = metadataCache;
        dispatcher.registerHandler(ApiKeys.LIST_OFFSETS, this);
    }


    @Override
    public void handle(Request req, RequestChannel requestChannel, KafkaProtocolClient client) {
        ListOffsetRequest request = (ListOffsetRequest) req.body();
        TopicPartition topicPartition;
        if(req.header().apiVersion() == 1){
            Map<TopicPartition, Long> p = request.partitionTimestamps();
            topicPartition = p.keySet().stream().findFirst().get();
        } else {
            Map<TopicPartition, ListOffsetRequest.PartitionData> p = request.offsetData();
            topicPartition = p.keySet().stream().findFirst().get();
        }
        Node target = metadataCache.getLeaderForTopicPartition(topicPartition);
        Struct response = client.sendSync(target, ApiKeys.LIST_OFFSETS, req.header().apiVersion(), request, 1000000000);
        log.trace("LIST_OFFSETS response = {}", response);
        ResponseHeader responseHeader = new ResponseHeader(req.header().correlationId());
        requestChannel.sendResponse(new Response(req, new ResponseSend(req.connectionId(), responseHeader, response)));
    }
}
