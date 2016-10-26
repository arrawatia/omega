package io.omega;

import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.types.Struct;
import org.apache.kafka.common.requests.MetadataRequest;
import org.apache.kafka.common.requests.MetadataResponse;
import org.apache.kafka.common.requests.ResponseHeader;
import org.apache.kafka.common.requests.ResponseSend;

import java.util.ArrayList;

import io.omega.client.KafkaProtocolClient;

/**
 * Created by sumit on 10/26/16.
 */
public class KafkaApis {
    private final KafkaProtocolClient client;

    public KafkaApis(KafkaProtocolClient client) {
        this.client = client;
    }

    public void handle(Request req, RequestChannel requestChannel) {
        try {

            System.out.println("-----connectionId-----" + req.connectionId());
            System.out.println("-----processor-----" + req.processor());
            System.out.println("-----securityProtocol-----" + req.securityProtocol());

            System.out.println("-----header-----" + req.header());
            System.out.println("-----header-----" + req.body());
        } catch (Exception e) {
            e.printStackTrace();
        }

        if (req.header().apiKey() == 3) {

//            val metadataRequest = request.body.asInstanceOf[MetadataRequest]
//            val requestVersion = request.header.apiVersion()
//
//            val topics =
//            // Handle old metadata request logic. Version 0 has no way to specify "no topics".
//            if (requestVersion == 0) {
//                if (metadataRequest.topics() == null || metadataRequest.topics().isEmpty)
//                    metadataCache.getAllTopics()
//                else
//                    metadataRequest.topics.asScala.toSet
//            } else {
//                if (metadataRequest.isAllTopics)
//                    metadataCache.getAllTopics()
//                else
//                    metadataRequest.topics.asScala.toSet
//            }
            MetadataRequest request = (MetadataRequest)req.body();
            System.out.println("-----request-----" + request.toString());

            int timeOutInMs = 1000;
            Struct responseBody = client.sendAnyNode(ApiKeys.METADATA, request, timeOutInMs);
            System.out.println("-----responseBody-----" + responseBody);


            MetadataResponse response = new MetadataResponse(responseBody);
            ResponseHeader responseHeader = new ResponseHeader(req.header().correlationId());


            requestChannel.sendResponse(new Response(req, new ResponseSend(req.connectionId(), responseHeader, responseBody)));
        }

    }
}

