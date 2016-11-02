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
            System.out.println("\n\n");
            System.out.println("-----API_KEY-----" + ApiKeys.forId(req.header().apiKey()));
            System.out.println("-----API_VERSION-----" + req.header().apiVersion());
            System.out.println("-----connectionId-----" + req.connectionId());
            System.out.println("-----processor-----" + req.processor());
            System.out.println("-----securityProtocol-----" + req.securityProtocol());
            System.out.println("-----header-----" + req.header());
            System.out.println("-----body-----" + req.body());
            System.out.println("\n\n");
            KafkaApiHandler handler = handlerTable[req.header().apiKey()];
            handler.handle(req, requestChannel, this.client);
            if (handler == null) {
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

            log.error("Error when handling request {}", req.toString(), t);
//            t.printStackTrace();
        }
    }

    @Override
    public void registerHandler(ApiKeys apiKey, KafkaApiHandler handler) {
        handlerTable[apiKey.id] = handler;
    }
}


