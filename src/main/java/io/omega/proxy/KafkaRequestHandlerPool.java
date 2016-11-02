package io.omega.proxy;

import org.apache.kafka.common.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

import io.omega.server.RequestChannel;

public class KafkaRequestHandlerPool {
    private static final Logger log = LoggerFactory.getLogger(KafkaRequestHandlerPool.class);

    private final KafkaRequestHandler[] runnables;
    private final Thread[] threads;
    private final int brokerId;
    private final RequestChannel requestChannel;
    private final int numThreads;
    private final Map<String, String> cfg;

    public KafkaRequestHandlerPool(int brokerId, RequestChannel requestChannel, Map<String, String> cfg, int numThreads) {
        this.brokerId = brokerId;
        this.requestChannel = requestChannel;
        this.cfg = cfg;
        this.numThreads = numThreads;
        this.threads = new Thread[numThreads];
        this.runnables = new KafkaRequestHandler[numThreads];

    /* a meter to track the average free capacity of the request handlers */
//        private val aggregateIdleMeter = newMeter("RequestHandlerAvgIdlePercent", "percent", TimeUnit.NANOSECONDS)

//        this.logIdent = "[Kafka Request Handler on Broker " + brokerId + "], "
    }
    public void startup(){
        for (int i = 0; i < numThreads; i++) {
//            runnables[i] = new KafkaRequestHandler(i, brokerId, aggregateIdleMeter, numThreads, requestChannel, factory);
            runnables[i] = new KafkaRequestHandler(i, brokerId, numThreads, requestChannel, RequestDispatcherFactory.create(cfg));
            threads[i] = Utils.daemonThread("kafka-request-handler-" + i, runnables[i]);
            threads[i].start();
        }
    }

    public void shutdown() {
        log.info("shutting down");
        for (KafkaRequestHandler handler : runnables)
            handler.shutdown();
        for (Thread thread : threads)
            try {
                thread.join();
            } catch (InterruptedException e) {
                log.error("", e);
            }
        log.info("shut down completely");
    }
}


