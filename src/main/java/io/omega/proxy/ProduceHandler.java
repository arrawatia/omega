package io.omega.proxy;

import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.types.Struct;
import org.apache.kafka.common.requests.ProduceRequest;
import org.apache.kafka.common.requests.ResponseHeader;
import org.apache.kafka.common.requests.ResponseSend;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.omega.client.KafkaProtocolClient;
import io.omega.server.Request;
import io.omega.server.RequestChannel;
import io.omega.server.Response;

public class ProduceHandler implements KafkaApiHandler {
    private static final Logger log = LoggerFactory.getLogger(ProduceHandler.class);

    KafkaRequestDispatcher dispatcher;
    MetadataCache metadataCache;

    public ProduceHandler(KafkaRequestDispatcher dispatcher, MetadataCache metadataCache) {
        this.dispatcher = dispatcher;
        this.metadataCache = metadataCache;
        dispatcher.registerHandler(ApiKeys.PRODUCE, this);
    }


    @Override
    public void handle(Request req, RequestChannel requestChannel, KafkaProtocolClient client) {
        ProduceRequest request = (ProduceRequest) req.body();
        TopicPartition topicPartition = request.partitionRecords().keySet().stream().findFirst().get();
        Node target = metadataCache.getLeaderForTopicPartition(topicPartition);
        Struct response = client.sendSync(target, ApiKeys.PRODUCE, req.header().apiVersion(), request, 1000000000);
        log.trace("PRODUCE response = {}", response);
        ResponseHeader responseHeader = new ResponseHeader(req.header().correlationId());
        requestChannel.sendResponse(new Response(req, new ResponseSend(req.connectionId(), responseHeader, response)));
    }
}
