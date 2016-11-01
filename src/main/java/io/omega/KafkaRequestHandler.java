package io.omega;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KafkaRequestHandler implements Runnable {

    private static final Logger log = LoggerFactory.getLogger(KafkaRequestHandler.class);

    private final int id;
    private final int brokerId;

    //    Meter aggregateIdleMeter;
    private final int totalHandlerThreads;
    private final RequestChannel requestChannel;
    private final KafkaApis apis;

    public KafkaRequestHandler(int id, int brokerId, int totalHandlerThreads, RequestChannel requestChannel, KafkaApis apis) {
        this.id = id;
        this.brokerId = brokerId;
        this.totalHandlerThreads = totalHandlerThreads;
        this.requestChannel = requestChannel;
        this.apis = apis;
    }

    //    apis: KafkaApis)   {
//        this.logIdent = "[Kafka Request Handler " + id + " on Broker " + brokerId + "], "

    public void run() {
        while (true) {
            try {
                Request req = null;
                while (req == null) {
                    // We use a single meter for aggregate idle percentage for the thread pool.
                    // Since meter is calculated as total_recorded_value / time_window and
                    // time_window is independent of the number of threads, each recorded idle
                    // time should be discounted by # threads.
//                         startSelectTime = SystemTime.nanoseconds
                    req = requestChannel.receiveRequest(300L);
//                        val idleTime = SystemTime.nanoseconds - startSelectTime
//                        aggregateIdleMeter.mark(idleTime / totalHandlerThreads)
                }

                if (req.equals(RequestChannel.allDone())) {
                    log.debug("Kafka request handler {} on broker {} received shut down command", id, brokerId);
                    return;
                }
//                    req.requestDequeueTimeMs = SystemTime.milliseconds
                log.trace("Kafka request handler {} on broker %d handling request {}", id, brokerId, req);
                apis.handle(req, requestChannel);
            } catch (Throwable e) {
                log.error("Exception when handling request", e);
            }
        }
    }

    public void shutdown() {
        requestChannel.sendRequest(RequestChannel.allDone());
    }
}

