package io.omega.server;

import org.apache.kafka.common.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.omega.KafkaApiHandler;

public class KafkaRequestHandlerPool {
    private static final Logger log = LoggerFactory.getLogger(KafkaRequestHandlerPool.class);

    private final KafkaRequestHandler[] runnables;
    private final Thread[] threads;
    private final int brokerId;
    private final RequestChannel requestChannel;
    private final KafkaApiHandler apis;
    private final int numThreads;

    public KafkaRequestHandlerPool(int brokerId, RequestChannel requestChannel, KafkaApiHandler apis, int numThreads) {
        this.brokerId = brokerId;
        this.requestChannel = requestChannel;
        this.apis = apis;
        this.numThreads = numThreads;

    /* a meter to track the average free capacity of the request handlers */
//        private val aggregateIdleMeter = newMeter("RequestHandlerAvgIdlePercent", "percent", TimeUnit.NANOSECONDS)

//        this.logIdent = "[Kafka Request Handler on Broker " + brokerId + "], "
        threads = new Thread[numThreads];
        runnables = new KafkaRequestHandler[numThreads];
        for (int i = 0; i < numThreads; i++) {

//            runnables[i] = new KafkaRequestHandler(i, brokerId, aggregateIdleMeter, numThreads, requestChannel, apis);
            runnables[i] = new KafkaRequestHandler(i, brokerId, numThreads, requestChannel, apis);
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


