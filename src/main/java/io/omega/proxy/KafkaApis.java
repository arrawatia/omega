package io.omega.proxy;

import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.requests.AbstractRequestResponse;
import org.apache.kafka.common.requests.ResponseHeader;
import org.apache.kafka.common.requests.ResponseSend;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

import io.omega.client.KafkaProtocolClient;
import io.omega.server.Request;
import io.omega.server.RequestChannel;
import io.omega.server.Response;

public class KafkaApis implements KafkaRequestDispatcher {
    private static final Logger log = LoggerFactory.getLogger(KafkaApis.class);

    private final KafkaProtocolClient client;
    private final KafkaApiHandler[] handlerTable = new KafkaApiHandler[ApiKeys.MAX_API_KEY];

    public KafkaApis(Map<String, String> cfg) {
        this.client = new KafkaProtocolClient(cfg);
    }


    @Override
    public void dispatch(Request req, RequestChannel requestChannel) {
        try {
            log.trace("Dispatching request : \n\n{}\n\n", req);
            KafkaApiHandler handler = handlerTable[req.header().apiKey()];
            if (handler == null) {
                throw new RuntimeException("Invalid request. API key : " + req.header().apiKey());
            }
            handler.handle(req, requestChannel, this.client);
        } catch (Throwable t) {
            AbstractRequestResponse response = req.body().getErrorResponse(req.header().apiVersion(), t);
            ResponseHeader respHeader = new ResponseHeader(req.header().correlationId());

            //If request doesn't have a default error response, we just close the connection. For example, when produce request has acks set to 0
            if (response == null)
                requestChannel.closeConnection(req.processor(), req);
            else
                requestChannel.sendResponse(new Response(req, new ResponseSend(req.connectionId(), respHeader, response)));

            log.error("Error when handling request {}", req.toString(), t);
        }
    }

    @Override
    public void registerHandler(ApiKeys apiKey, KafkaApiHandler handler) {
        handlerTable[apiKey.id] = handler;
    }
}


