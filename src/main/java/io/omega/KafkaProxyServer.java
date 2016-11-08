package io.omega;


import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.utils.SystemTime;
import org.apache.kafka.common.utils.Utils;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;

import io.omega.network.SocketServer;
import io.omega.proxy.KafkaRequestHandlerPool;

public class KafkaProxyServer {

    public static void main(String[] args) throws IOException, InterruptedException {
        try {
            Map<String, String> proxycfg = Utils.propsToStringMap(Utils.loadProps(args[0]));
//            proxycfg.put(ProxyServerConfig.ListenersProp, "PLAINTEXT://0.0.0.0:9088");
//            Map<String, String> cfg = new HashMap<>();
//            cfg.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, "localhost:9095");

            ProxyServerConfig config = new ProxyServerConfig(proxycfg);
            SocketServer socketServer;
            KafkaRequestHandlerPool requestHandlerPool;
            CountDownLatch shutdownLatch = new CountDownLatch(1);

            socketServer = new SocketServer(config, new Metrics(), new SystemTime());
            requestHandlerPool = new KafkaRequestHandlerPool(-1, socketServer.requestChannel(), config, config.getInt(ProxyServerConfig.NumIoThreadsProp));

            // attach shutdown handler to catch control-c
            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                requestHandlerPool.shutdown();
                socketServer.shutdown();
                shutdownLatch.countDown();
            }));

            /* start processing requests */
            socketServer.startup();
            requestHandlerPool.startup();
            shutdownLatch.await();
        } catch (Throwable e) {
            e.printStackTrace();
            System.exit(1);
        }
        System.exit(0);
    }
}

