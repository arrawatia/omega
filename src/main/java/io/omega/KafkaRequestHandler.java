package io.omega;

public class KafkaRequestHandler implements Runnable {

    int id;
    int brokerId;


    //    Meter aggregateIdleMeter;
    int totalHandlerThreads;
    RequestChannel requestChannel;
    KafkaApis apis;

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
//                        debug("Kafka request handler %d on broker %d received shut down command".format(id, brokerId))
                    return;
                }
//                    req.requestDequeueTimeMs = SystemTime.milliseconds
//                    trace("Kafka request handler %d on broker %d handling request %s".format(id, brokerId, req))
                    apis.handle(req, requestChannel);
            } catch (Throwable e) {
//                     error("Exception when handling request", e)
            }
        }
    }

    public void shutdown() {
        requestChannel.sendRequest(RequestChannel.allDone());
    }
}

