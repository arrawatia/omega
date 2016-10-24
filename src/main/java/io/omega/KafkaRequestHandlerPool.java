package io.omega;

/**
 * Created by sumit on 10/24/16.
 */
public class KafkaRequestHandlerPool {

    class KafkaRequestHandlerPool(val brokerId: Int,
    val requestChannel: RequestChannel,
    val apis: KafkaApis,
    numThreads: Int) extends Logging with KafkaMetricsGroup {

  /* a meter to track the average free capacity of the request handlers */
        private val aggregateIdleMeter = newMeter("RequestHandlerAvgIdlePercent", "percent", TimeUnit.NANOSECONDS)

        this.logIdent = "[Kafka Request Handler on Broker " + brokerId + "], "
        val threads = new Array[Thread](numThreads)
                val runnables = new Array[KafkaRequestHandler](numThreads)
        for(i <- 0 until numThreads) {
            runnables(i) = new KafkaRequestHandler(i, brokerId, aggregateIdleMeter, numThreads, requestChannel, apis)
            threads(i) = Utils.daemonThread("kafka-request-handler-" + i, runnables(i))
            threads(i).start()
        }

        def shutdown() {
            info("shutting down")
            for(handler <- runnables)
                handler.shutdown
            for(thread <- threads)
                thread.join
            info("shut down completely")
        }
    }

}
