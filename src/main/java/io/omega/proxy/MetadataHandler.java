package io.omega.proxy;

import org.apache.kafka.common.Node;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.SecurityProtocol;
import org.apache.kafka.common.protocol.types.Struct;
import org.apache.kafka.common.requests.MetadataRequest;
import org.apache.kafka.common.requests.MetadataResponse;
import org.apache.kafka.common.requests.ResponseHeader;
import org.apache.kafka.common.requests.ResponseSend;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import io.omega.ProxyServerConfig;
import io.omega.client.KafkaProtocolClient;
import io.omega.server.EndPoint;
import io.omega.server.Request;
import io.omega.server.RequestChannel;
import io.omega.server.Response;

public class MetadataHandler implements KafkaApiHandler {
    private final ProxyServerConfig config;
    KafkaRequestDispatcher dispatcher;
    MetadataCache metadataCache;

    public MetadataHandler(KafkaRequestDispatcher dispatcher, MetadataCache metadataCache, ProxyServerConfig cfg) {
        this.dispatcher = dispatcher;
        this.metadataCache = metadataCache;
        this.config = cfg;
        dispatcher.registerHandler(ApiKeys.METADATA, this);
    }

    @Override
    public void handle(Request req, RequestChannel requestChannel, KafkaProtocolClient client) {

        MetadataRequest request = (MetadataRequest) req.body();
//            System.out.println("-----API_KEY-----" + ApiKeys.forId(req.header().apiKey()) +
//                    "-----request-----" + request.toString());

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
        metadataCache.update(response);
        int version = req.header().apiVersion();
//                System.out.println("-----brokers -----" + metadata);


        // TODO : Support more than one protocol.
        Map<SecurityProtocol, EndPoint> endpoints = EndPoint.getEndpoints(config.getList(ProxyServerConfig.ListenersProp));
        EndPoint plainTextEndpoint = endpoints.get(SecurityProtocol.PLAINTEXT);

        List<Node> proxyBrokers = new ArrayList<>();
        for (Node b : brokers) {
            proxyBrokers.add(new Node(b.id(), plainTextEndpoint.host(), plainTextEndpoint.port(), b.rack()));
        }


        MetadataResponse proxyResponse = new MetadataResponse(proxyBrokers, clusterId, controller, metadata, version);
//                System.out.println("-----proxy responseBody-----" + proxyResponse);

        requestChannel.sendResponse(new Response(req, new ResponseSend(req.connectionId(), responseHeader, proxyResponse.toStruct())));
    }
}
