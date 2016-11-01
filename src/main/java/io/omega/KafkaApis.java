package io.omega;

import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.types.Struct;
import org.apache.kafka.common.requests.MetadataRequest;
import org.apache.kafka.common.requests.MetadataResponse;
import org.apache.kafka.common.requests.ProduceRequest;
import org.apache.kafka.common.requests.ResponseHeader;
import org.apache.kafka.common.requests.ResponseSend;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import io.omega.client.KafkaProtocolClient;

public class KafkaApis {
    private static final Logger log = LoggerFactory.getLogger(KafkaApis.class);

    private final KafkaProtocolClient client;

    public HashMap<TopicPartition, Node> topicLeaderMetadataCache = new HashMap<>();

    public KafkaApis(KafkaProtocolClient client) {
        this.client = client;
    }

    private void populateMetadataCache(List<MetadataResponse.TopicMetadata> metadata) {
        for (MetadataResponse.TopicMetadata m : metadata) {
            String topic = m.topic();
            m.partitionMetadata().stream().forEach(p ->
                topicLeaderMetadataCache.put(new TopicPartition(topic, p.partition()), p.leader())
            );
        }
        log.trace("Metadata cache : {} " , this.topicLeaderMetadataCache);
    }

    public void handle(Request req, RequestChannel requestChannel) {
        System.out.println("-----connectionId-----" + req.connectionId());
        System.out.println("-----processor-----" + req.processor());
        System.out.println("-----securityProtocol-----" + req.securityProtocol());
        System.out.println("-----header-----" + req.header());
        System.out.println("-----header-----" + req.body());

        if (req.header().apiKey() == 3) {

            MetadataRequest request = (MetadataRequest) req.body();
            System.out.println("-----request-----" + request.toString());

            int timeOutInMs = 1000;
            Struct responseBody = client.sendAnyNode(ApiKeys.METADATA, req.header().apiVersion(), request, timeOutInMs);
            System.out.println("-----responseBody-----" + responseBody);

            ResponseHeader responseHeader = new ResponseHeader(req.header().correlationId());
            MetadataResponse response = new MetadataResponse(responseBody);

            List<Node> brokers = new ArrayList<>(response.brokers());
            String clusterId = response.clusterId();
            int controller = -1;
            if (response.controller() != null) {
                controller = response.controller().id();

            }
            List<MetadataResponse.TopicMetadata> metadata = new ArrayList<>(response.topicMetadata());
            populateMetadataCache(metadata);
            int version = req.header().apiVersion();
            System.out.println("-----brokers -----" + metadata);


            List<Node> proxyBrokers = new ArrayList<>();
            for (Node b : brokers) {
                proxyBrokers.add(new Node(b.id(), b.host(), 9088, b.rack()));
            }

            MetadataResponse proxyResponse = new MetadataResponse(proxyBrokers, clusterId, controller, metadata, version);
            System.out.println("-----proxy responseBody-----" + proxyResponse);

            requestChannel.sendResponse(new Response(req, new ResponseSend(req.connectionId(), responseHeader, proxyResponse.toStruct())));
        }

        if (ApiKeys.forId(req.header().apiKey()) == ApiKeys.PRODUCE) {
            ProduceRequest request = (ProduceRequest) req.body();
            Map<TopicPartition, ByteBuffer> p = request.partitionRecords();
            p.keySet().stream().forEach((TopicPartition tp) -> {
                System.out.println(tp);
                Struct response = client.sendSync(topicLeaderMetadataCache.get(tp), ApiKeys.PRODUCE, request, 1000);
                log.trace("Produce response = {}", response);
                ResponseHeader responseHeader = new ResponseHeader(req.header().correlationId());
                requestChannel.sendResponse(new Response(req, new ResponseSend(req.connectionId(), responseHeader, response)));
            });

//            topicMetadata = client.getTopicMetadata(request.partitionRecords())
        }

    }
}

