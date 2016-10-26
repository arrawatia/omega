package io.omega;

import org.apache.kafka.common.utils.Utils;

public class KafkaRequestHandlerPool {
    private final KafkaRequestHandler[] runnables;
    private final Thread[] threads;
    int brokerId;
    RequestChannel requestChannel;
    KafkaApis apis;
    int numThreads;

    public KafkaRequestHandlerPool(int brokerId, RequestChannel requestChannel, KafkaApis apis, int numThreads) {
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
//            info("shutting down")
        for (KafkaRequestHandler handler : runnables)
            handler.shutdown();
        for (Thread thread : threads)
            try {
                thread.join();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
//            info("shut down completely");
    }
}


