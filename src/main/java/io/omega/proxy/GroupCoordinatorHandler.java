package io.omega.proxy;

import org.apache.kafka.common.Node;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.SecurityProtocol;
import org.apache.kafka.common.protocol.types.Struct;
import org.apache.kafka.common.requests.GroupCoordinatorRequest;
import org.apache.kafka.common.requests.GroupCoordinatorResponse;
import org.apache.kafka.common.requests.ResponseHeader;
import org.apache.kafka.common.requests.ResponseSend;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

import io.omega.ProxyServerConfig;
import io.omega.client.KafkaProtocolClient;
import io.omega.server.EndPoint;
import io.omega.server.Request;
import io.omega.server.RequestChannel;
import io.omega.server.Response;

public class GroupCoordinatorHandler implements KafkaApiHandler {

    private static final Logger log = LoggerFactory.getLogger(GroupCoordinatorHandler.class);
    private final ProxyServerConfig config;

    KafkaRequestDispatcher dispatcher;
    MetadataCache metadataCache;

    public GroupCoordinatorHandler(KafkaRequestDispatcher dispatcher, MetadataCache metadataCache, ProxyServerConfig cfg) {
        this.dispatcher = dispatcher;
        this.metadataCache = metadataCache;
        this.config = cfg;
        dispatcher.registerHandler(ApiKeys.GROUP_COORDINATOR, this);
    }
    @Override
    public void handle(Request req, RequestChannel requestChannel, KafkaProtocolClient client) {
        GroupCoordinatorRequest request = (GroupCoordinatorRequest) req.body();
        Struct responseBody = client.sendAnyNode(ApiKeys.GROUP_COORDINATOR, req.header().apiVersion(), request, 1000000);
        GroupCoordinatorResponse response = new GroupCoordinatorResponse(responseBody);
        Node c = response.node();

        // TODO : Support more than one protocol.
        Map<SecurityProtocol, EndPoint> endpoints = EndPoint.getEndpoints(config.getList(ProxyServerConfig.ListenersProp));
        EndPoint plainTextEndpoint = endpoints.get(SecurityProtocol.PLAINTEXT);

        GroupCoordinatorResponse proxyResponse = new GroupCoordinatorResponse(response.errorCode(), new Node(c.id(), plainTextEndpoint.host(), plainTextEndpoint.port(), c.rack()));

        ResponseHeader responseHeader = new ResponseHeader(req.header().correlationId());
        requestChannel.sendResponse(new Response(req, new ResponseSend(req.connectionId(), responseHeader, proxyResponse.toStruct())));
    }
}
