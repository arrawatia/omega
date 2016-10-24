package io.omega;

/**
 * Created by sumit on 10/24/16.
 */
public class KafkaRequestHandler {

    class KafkaRequestHandler(id: Int,
    brokerId: Int,
    val aggregateIdleMeter: Meter,
    val totalHandlerThreads: Int,
    val requestChannel: RequestChannel,
    apis: KafkaApis) extends Runnable with Logging {
        this.logIdent = "[Kafka Request Handler " + id + " on Broker " + brokerId + "], "

        def run() {
            while(true) {
                try {
                    var req : RequestChannel.Request = null
                    while (req == null) {
                        // We use a single meter for aggregate idle percentage for the thread pool.
                        // Since meter is calculated as total_recorded_value / time_window and
                        // time_window is independent of the number of threads, each recorded idle
                        // time should be discounted by # threads.
                        val startSelectTime = SystemTime.nanoseconds
                        req = requestChannel.receiveRequest(300)
                        val idleTime = SystemTime.nanoseconds - startSelectTime
                        aggregateIdleMeter.mark(idleTime / totalHandlerThreads)
                    }

                    if(req eq RequestChannel.AllDone) {
                        debug("Kafka request handler %d on broker %d received shut down command".format(
                                id, brokerId))
                        return
                    }
                    req.requestDequeueTimeMs = SystemTime.milliseconds
                    trace("Kafka request handler %d on broker %d handling request %s".format(id, brokerId, req))
                    apis.handle(req)
                } catch {
                    case e: Throwable => error("Exception when handling request", e)
                }
            }
        }

        def shutdown(): Unit = requestChannel.sendRequest(RequestChannel.AllDone)
    }
}
