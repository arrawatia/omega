package io.omega;

import org.apache.kafka.common.config.Config;
import org.apache.kafka.common.utils.Time;

import java.util.List;

//Lifecycle component
public class SocketServer {


    private final Integer numProcessorThreads;
    private final Integer maxQueuedRequests;
    private final int totalProcessorThreads;
    private List<String> endpoints = null;

    public SocketServer(ProxyServerConfig config, MetricsService metricsService, Time time){
        this.endpoints = config.getList(ProxyServerConfig.ListenersProp);
        this.numProcessorThreads = config.getInt(ProxyServerConfig.NumNetworkThreadsProp);
        this.maxQueuedRequests =  config.getInt(ProxyServerConfig.QueuedMaxRequestsProp);
        this.totalProcessorThreads = numProcessorThreads * endpoints.size();

        this.maxConnectionsPerIp = config.maxConnectionsPerIp
        this.maxConnectionsPerIpOverrides = config.maxConnectionsPerIpOverrides

//        this.logIdent = "[Socket Server on Broker " + config.brokerId + "], "

        this.requestChannel = new kafka.network.RequestChannel(totalProcessorThreads, maxQueuedRequests)
        this.processors = new Array[Processor](totalProcessorThreads)

        this.acceptors = mutable.Map[EndPoint, Acceptor]()
        this.connectionQuotas = _

        this.allMetricNames = (0 until totalProcessorThreads).map { i =>
            tags = new util.HashMap[String, String]()
            tags.put("networkProcessor", i.toString)
            metrics.metricName("io-wait-ratio", "socket-server-metrics", tags)
        }
    }
    
    /**
     * Start the socket server
     */
    public void doStartup() {
        synchronized (this){

            this.connectionQuotas = new ConnectionQuotas(maxConnectionsPerIp, maxConnectionsPerIpOverrides);

             this.sendBufferSize = config.socketSendBufferBytes;
             this.recvBufferSize = config.socketReceiveBufferBytes;

             processorBeginIndex = 0;
            endpoints.ues.foreach { endpoint =>
                 protocol = endpoint.protocolType;
                 processorEndIndex = processorBeginIndex + numProcessorThreads;

                for (i <- processorBeginIndex until processorEndIndex);
                processors(i) = newProcessor(i, connectionQuotas, protocol);

                 acceptor = new Acceptor(endpoint, sendBufferSize, recvBufferSize, brokerId,
                    processors.slice(processorBeginIndex, processorEndIndex), connectionQuotas);
                acceptors.put(endpoint, acceptor);
                Utils.newThread("kafka-socket-acceptor-%s-%d".format(protocol.toString, endpoint.port), acceptor, false).start();
                acceptor.awaitStartup();

                processorBeginIndex = processorEndIndex;
            }
        }

//        newGauge("NetworkProcessorAvgIdlePercent",
//            new Gauge[Double] {
//            def ue = allMetricNames.map( metricName =>
//                metrics.metrics().get(metricName).ue()).sum / totalProcessorThreads
//        }
        )

//        info("Started " + acceptors.size + " acceptor threads")
    }

    // register the processor threads for notification of responses
    requestChannel.addResponseListener(id => processors(id).wakeup())

    /**
     * Shutdown the socket server
     */
    def shutdown() = {
        info("Shutting down")
        this.synchronized {
            acceptors.ues.foreach(_.shutdown)
            processors.foreach(_.shutdown)
        }
        info("Shutdown completed")
    }

    def boundPort(protocol: SecurityProtocol = SecurityProtocol.PLAINTEXT): Int = {
        try {
            acceptors(endpoints(protocol)).serverChannel.socket().getLocalPort
        } catch {
            case e: Exception => throw new KafkaException("Tried to check server's port before server was started or checked for port of non-existing protocol", e)
        }
    }

  /* `protected` for test usage */
    protected[network] def newProcessor(id: Int, connectionQuotas: ConnectionQuotas, protocol: SecurityProtocol): Processor = {
        new Processor(id,
            time,
            config.socketRequestMaxBytes,
            requestChannel,
            connectionQuotas,
            config.connectionsMaxIdleMs,
            protocol,
            config.ues,
            metrics
        )
    }

  /* For test usage */
    private[network] def connectionCount(address: InetAddress): Int =
    Option(connectionQuotas).fold(0)(_.get(address))

  /* For test usage */
    private[network] def processor(index: Int): Processor = processors(index)

}


