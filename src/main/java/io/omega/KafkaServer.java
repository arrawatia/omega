package io.omega;


import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.utils.SystemTime;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import io.omega.client.KafkaProtocolClient;

// Lifecycle component
public class KafkaServer {


    public static void main(String[] args) throws IOException, InterruptedException {

        Map<String, String> proxycfg = new HashMap<>();
        proxycfg.put(ProxyServerConfig.ListenersProp, "PLAINTEXT://0.0.0.0:9088");

        ProxyServerConfig config = new ProxyServerConfig(proxycfg);
        System.out.println(config.values());
        System.out.println(config.getInt(ProxyServerConfig.SocketReceiveBufferBytesProp));
    KafkaApis apis;
    SocketServer socketServer;
    KafkaRequestHandlerPool requestHandlerPool ;

        Map<String, String> cfg = new HashMap<>();
        cfg.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        KafkaProtocolClient client = new KafkaProtocolClient(cfg);



    AtomicInteger correlationId  = new AtomicInteger(0);
        socketServer = new SocketServer(config, new Metrics(), new SystemTime());
    socketServer.startup();


     /* start processing requests */
    apis = new KafkaApis(client);

    requestHandlerPool = new KafkaRequestHandlerPool(-1, socketServer.requestChannel(), apis, config.getInt(ProxyServerConfig.NumIoThreadsProp));

        while (true){
            Thread.sleep(10000);
        }
    }

//    /* tell everyone we are alive */
//    val listeners = config.advertisedListeners.map {case(protocol, endpoint) =>
//        if (endpoint.port == 0)
//            (protocol, EndPoint(endpoint.host, socketServer.boundPort(protocol), endpoint.protocolType))
//        else
//        (protocol, endpoint)
//    }



}
