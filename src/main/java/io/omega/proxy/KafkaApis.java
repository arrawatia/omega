package io.omega.proxy;

import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.types.Struct;
import org.apache.kafka.common.requests.AbstractRequestResponse;
import org.apache.kafka.common.requests.FetchRequest;
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

import io.omega.KafkaApiHandler;
import io.omega.client.KafkaProtocolClient;
import io.omega.server.Request;
import io.omega.server.RequestChannel;
import io.omega.server.Response;

public class KafkaApis implements KafkaApiHandler {
    private static final Logger log = LoggerFactory.getLogger(KafkaApis.class);

    private final KafkaProtocolClient client;

    public HashMap<TopicPartition, Node> topicLeaderMetadataCache = new HashMap<>();

    public KafkaApis(Map<String, String> cfg) {
        this.client = new KafkaProtocolClient(cfg);
    }

    private void populateMetadataCache(List<MetadataResponse.TopicMetadata> metadata) {
        for (MetadataResponse.TopicMetadata m : metadata) {
            String topic = m.topic();
            m.partitionMetadata().stream().forEach(p ->
                    topicLeaderMetadataCache.put(new TopicPartition(topic, p.partition()), p.leader())
            );
        }
        log.trace("Metadata cache : {} ", this.topicLeaderMetadataCache);
    }

    @Override
    public void handle(Request req, RequestChannel requestChannel) {
        try {
            System.out.println("\n\n");
            System.out.println("-----API_KEY-----" + ApiKeys.forId(req.header().apiKey()));
            System.out.println("-----API_VERSION-----" + req.header().apiVersion());
            System.out.println("-----connectionId-----" + req.connectionId());
            System.out.println("-----processor-----" + req.processor());
            System.out.println("-----securityProtocol-----" + req.securityProtocol());
            System.out.println("-----header-----" + req.header());
            System.out.println("-----body-----" + req.body());
            System.out.println("\n\n");

            if (req.header().apiKey() == 3) {

                MetadataRequest request = (MetadataRequest) req.body();
                System.out.println("-----API_KEY-----" + ApiKeys.forId(req.header().apiKey()) +
                                    "-----request-----" + request.toString());

                int timeOutInMs = 1000;
                Struct responseBody = client.sendAnyNode(ApiKeys.METADATA, req.header().apiVersion(), request, timeOutInMs);
//                System.out.println("-----responseBody-----" + responseBody);

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
//                System.out.println("-----brokers -----" + metadata);


                List<Node> proxyBrokers = new ArrayList<>();
                for (Node b : brokers) {
                    proxyBrokers.add(new Node(b.id(), b.host(), 9088, b.rack()));
                }

                MetadataResponse proxyResponse = new MetadataResponse(proxyBrokers, clusterId, controller, metadata, version);
//                System.out.println("-----proxy responseBody-----" + proxyResponse);

                requestChannel.sendResponse(new Response(req, new ResponseSend(req.connectionId(), responseHeader, proxyResponse.toStruct())));
            } else if (ApiKeys.forId(req.header().apiKey()) == ApiKeys.PRODUCE) {
                ProduceRequest request = (ProduceRequest) req.body();
                Map<TopicPartition, ByteBuffer> p = request.partitionRecords();
                Node target = topicLeaderMetadataCache.get(p.keySet().toArray()[0]);
//                p.keySet().stream().forEach((TopicPartition tp) -> {
                    System.out.println(target);
                    Struct response = client.sendSync(target, ApiKeys.PRODUCE, req.header().apiVersion(), request, 100000);
                    log.trace("PRODUCE response = {}", response);
                    ResponseHeader responseHeader = new ResponseHeader(req.header().correlationId());
                    requestChannel.sendResponse(new Response(req, new ResponseSend(req.connectionId(), responseHeader, response)));
//                });
            } else if (ApiKeys.forId(req.header().apiKey()) == ApiKeys.FETCH) {
                FetchRequest request = (FetchRequest) req.body();
                Map<TopicPartition, FetchRequest.PartitionData> p = request.fetchData();
                Node target = topicLeaderMetadataCache.get(p.keySet().toArray()[0]);

//                p.keySet().stream().forEach((TopicPartition tp) -> {
//                    System.out.println(tp);
                    Struct response = client.sendSync(target, ApiKeys.FETCH, req.header().apiVersion(), request, 1000000000);
                    log.trace("FETCH response = {}", response);
                    ResponseHeader responseHeader = new ResponseHeader(req.header().correlationId());
                    requestChannel.sendResponse(new Response(req, new ResponseSend(req.connectionId(), responseHeader, response)));
//                });
            } else {

                throw new RuntimeException("Invalid request id" + req.header().apiKey());
            }
        } catch (Throwable t) {
            AbstractRequestResponse response = req.body().getErrorResponse(req.header().apiVersion(), t);
            ResponseHeader respHeader = new ResponseHeader(req.header().correlationId());

          /* If request doesn't have a default error response, we just close the connection.
             For example, when produe request has acks set to 0 */
            if (response == null)
                requestChannel.closeConnection(req.processor(), req);
            else
                requestChannel.sendResponse(new Response(req, new ResponseSend(req.connectionId(), respHeader, response)));

            log.error("Error when handling request {}", req.toString(),  t);
            t.printStackTrace();
        }
    }
}


