package io.omega.network;

import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.protocol.SecurityProtocol;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetAddress;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import io.omega.server.EndPoint;
import io.omega.ProxyServerConfig;
import io.omega.server.RequestChannel;


public class SocketServer {
    private static final Logger log = LoggerFactory.getLogger(SocketServer.class);

    private final int numProcessorThreads;
    private final int maxQueuedRequests;
    private final int totalProcessorThreads;
    private final int maxConnectionsPerIp;
    private final Map<String, Integer> maxConnectionsPerIpOverrides;
    private final RequestChannel requestChannel;
    private final Processor[] processors;
    private final HashMap<EndPoint, Acceptor> acceptors;
    private final int sendBufferSize;
    private final int recvBufferSize;
    private final Time time;
    private final Metrics metrics;
    private final Integer socketRequestMaxBytes;
    private final Long connectionsMaxIdleMs;
    private final ProxyServerConfig config;
    private final Map<SecurityProtocol, EndPoint> endpoints;
    private ConnectionQuotas connectionQuotas;

    public static Map<String, String> getMapFromCsv(String csv) {
        if(csv.isEmpty()){
            return new HashMap<>();
        }
        Map<String, String> result =
                Arrays.stream(csv.split("\\s*,\\s*"))
                        .map(line -> line.split(","))
                        .collect(Collectors.toMap(line -> line[0], line -> line[1]));
        return result;
    }

    public RequestChannel requestChannel() {
        return requestChannel;
    }

    public SocketServer(ProxyServerConfig config, Metrics metrics, Time time) {
        this.endpoints = getListeners(config.getList(ProxyServerConfig.ListenersProp));
        this.numProcessorThreads = config.getInt(ProxyServerConfig.NumNetworkThreadsProp);
        this.maxQueuedRequests = config.getInt(ProxyServerConfig.QueuedMaxRequestsProp);
        this.totalProcessorThreads = numProcessorThreads * endpoints.size();
        this.time = time;
        this.metrics = metrics;
        this.config = config;

        this.maxConnectionsPerIp = config.getInt(ProxyServerConfig.MaxConnectionsPerIpProp);
        this.maxConnectionsPerIpOverrides = getMapFromCsv(config.getString(ProxyServerConfig.MaxConnectionsPerIpOverridesProp)).entrySet().stream()
                .collect(Collectors.toMap(
                        e -> e.getKey(),
                        e -> Integer.parseInt(e.getValue())
                ));

//        this.logIdent = "[Socket Server on Broker " + config.brokerId + "], "

        this.requestChannel = new RequestChannel(totalProcessorThreads, maxQueuedRequests);
        this.processors = new Processor[totalProcessorThreads];
        this.acceptors = new HashMap<EndPoint, Acceptor>();
        this.sendBufferSize = config.getInt(ProxyServerConfig.SocketSendBufferBytesProp);
        this.recvBufferSize = config.getInt(ProxyServerConfig.SocketReceiveBufferBytesProp);
        this.socketRequestMaxBytes = config.getInt(ProxyServerConfig.SocketRequestMaxBytesProp);
        this.connectionsMaxIdleMs = config.getLong(ProxyServerConfig.ConnectionsMaxIdleMsProp);

//        this.allMetricNames = (0 until totalProcessorThreads).map { i =>
//            tags = new util.HashMap[String, String]()
//            tags.put("networkProcessor", i.toString)
//            metrics.metricName("io-wait-ratio", "socket-server-metrics", tags)
//        }
    }

    private Map<SecurityProtocol, EndPoint> getListeners(List<String> endpointStrings) {
        Map<SecurityProtocol, EndPoint> endpoints = new HashMap<>();
        for(String endpointString: endpointStrings) {
            EndPoint e = EndPoint.createEndPoint(endpointString);
            endpoints.put(e.protocolType(), e);
        }
        return endpoints;
    }

    /**
     * Start the socket server
     */
    public void startup() throws IOException{
        synchronized (this) {
            this.connectionQuotas = new ConnectionQuotas(maxConnectionsPerIp, maxConnectionsPerIpOverrides);
            int processorBeginIndex = 0;
            for (EndPoint endpoint : endpoints.values()) {
                SecurityProtocol protocol = endpoint.protocolType();
                int processorEndIndex = processorBeginIndex + numProcessorThreads;

                for (int i = processorBeginIndex; i < processorEndIndex; i++) {
                    processors[i] = newProcessor(i, connectionQuotas, protocol);
                }

                Acceptor acceptor = new Acceptor(endpoint, sendBufferSize, recvBufferSize, -1, Arrays.copyOfRange(processors, processorBeginIndex, processorEndIndex), connectionQuotas);
                acceptors.put(endpoint, acceptor);
                Utils.newThread("kafka-socket-acceptor-%s-%d".format(protocol.toString(), endpoint.port()), acceptor, false).start();
                acceptor.awaitStartup();
                processorBeginIndex = processorEndIndex;
            }
        }

//        newGauge("NetworkProcessorAvgIdlePercent",
//            new Gauge[Double] {
//            def ue = allMetricNames.map( metricName =>
//                metrics.metrics().get(metricName).ue()).sum / totalProcessorThreads
//        }


//        info("Started " + acceptors.size + " acceptor threads")
        // register the processor threads for notification of responses
        requestChannel.addResponseListener(id -> processors[id].wakeup());
    }

    /**
     * Shutdown the socket server
     */
    public synchronized void shutdown() {
        log.info("Shutting down");
        acceptors.values().stream().forEach(acceptor -> acceptor.shutdown());
        Arrays.stream(processors).forEach(processor -> processor.shutdown());
        log.info("Shutdown completed");
    }

    public int boundPort(SecurityProtocol protocol) {
        if (protocol == null)
            protocol = SecurityProtocol.PLAINTEXT;
        try {
            return acceptors.get(endpoints.get(protocol)).serverChannel.socket().getLocalPort();
        } catch (Exception e) {
            throw new RuntimeException("Tried to check server's port before server was started or checked for port of non-existing protocol", e);
        }
    }

    /* `protected` for test usage */
    protected Processor newProcessor(int id, ConnectionQuotas connectionQuotas, SecurityProtocol protocol) {
        return new Processor(id,
                time,
                this.socketRequestMaxBytes,
                requestChannel,
                connectionQuotas,
                connectionsMaxIdleMs,
                protocol,
                config.values(),
                metrics
        );
    }

    /* For test usage */
    private int connectionCount(InetAddress address) {
//        Option(connectionQuotas).fold(0) (_.get(address));
        return -1;
    }

    /* For test usage */
    private Processor processor(int index) {
        return this.processors[index];
    }

}


