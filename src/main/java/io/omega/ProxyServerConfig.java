package io.omega;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;

import java.util.Map;

public class ProxyServerConfig extends AbstractConfig {

    private static final ConfigDef CONFIG;

    //Defaults
    private static final int MessageSizeLength = 4;
    private static final int OffsetLength = 8;
    private static final int LogOverhead = MessageSizeLength + OffsetLength;
    private static final int MessageMaxBytes = 1000000 + LogOverhead;
    private static final int NumNetworkThreads = 3;
    private static final int NumIoThreads = 8;
    private static final int BackgroundThreads = 10;
    private static final int QueuedMaxRequests = 500;

    private static final int SocketSendBufferBytes = 100 * 1024;
    private static final int SocketReceiveBufferBytes = 100 * 1024;
    private static final int SocketRequestMaxBytes = 100 * 1024 * 1024;
    private static final int MaxConnectionsPerIp = Integer.MAX_VALUE;
    private static final String MaxConnectionsPerIpOverrides = "";
    private static final long ConnectionsMaxIdleMs = 10 * 60 * 1000L;
    private static final int RequestTimeoutMs = 30000;

    //Prop names
    public static final String MessageMaxBytesProp = "message.max.bytes";
    public static final String NumNetworkThreadsProp = "num.network.threads";
    public static final String NumIoThreadsProp = "num.io.threads";
    public static final String BackgroundThreadsProp = "background.threads";
    public static final String QueuedMaxRequestsProp = "queued.max.requests";
    public static final String RequestTimeoutMsProp = CommonClientConfigs.REQUEST_TIMEOUT_MS_CONFIG;
    public static final String ListenersProp = "listeners";
    public static final String AdvertisedListenersProp = "advertised.listeners";
    public static final String SocketSendBufferBytesProp = "socket.send.buffer.bytes";
    public static final String SocketReceiveBufferBytesProp = "socket.receive.buffer.bytes";
    public static final String SocketRequestMaxBytesProp = "socket.request.max.bytes";
    public static final String MaxConnectionsPerIpProp = "max.connections.per.ip";
    public static final String MaxConnectionsPerIpOverridesProp =
        "max.connections.per.ip.overrides";
    public static final String ConnectionsMaxIdleMsProp = "connections.max.idle.ms";

    // Docs
    public static final String MessageMaxBytesDoc =
        "The maximum size of message that the server can receive";
    public static final String NumNetworkThreadsDoc =
        "the number of network threads that the server uses for handling network requests";
    public static final String NumIoThreadsDoc =
        "The number of io threads that the server uses for carrying out network requests";
    public static final String BackgroundThreadsDoc =
        "The number of threads to use for various background processing tasks";
    public static final String QueuedMaxRequestsDoc =
        "The number of queued requests allowed before blocking the network threads";
    public static final String RequestTimeoutMsDoc = CommonClientConfigs.REQUEST_TIMEOUT_MS_DOC;
    public static final String ListenersDoc =
        "Listener List - Comma-separated list of URIs we will listen on and their protocols.\n" +
            " Specify hostname as 0.0.0.0 to bind to all interfaces.\n" +
            " Leave hostname empty to bind to default interface.\n" +
            " Examples of legal listener lists:\n" +
            " PLAINTEXT://myhost:9092,TRACE://:9091\n" +
            " PLAINTEXT://0.0.0.0:9092, TRACE://localhost:9093\n";
    public static final String AdvertisedListenersDoc =
        "Listeners to publish to ZooKeeper for clients to use, if different than the listeners "
            + "above."
            +
            " In IaaS environments, this may need to be different from the interface to which the"
            + " broker binds."
            +
            " If this is not set, the value for `listeners` will be used.";
    public static final String SocketSendBufferBytesDoc =
        "The SO_SNDBUF buffer of the socket sever sockets. If the value is -1, the OS default "
            + "will be used.";
    public static final String SocketReceiveBufferBytesDoc =
        "The SO_RCVBUF buffer of the socket sever sockets. If the value is -1, the OS default "
            + "will be used.";
    public static final String SocketRequestMaxBytesDoc =
        "The maximum number of bytes in a socket request";
    public static final String MaxConnectionsPerIpDoc =
        "The maximum number of connections we allow from each ip address";
    public static final String MaxConnectionsPerIpOverridesDoc =
        "Per-ip or hostname overrides to the default maximum number of connections";
    public static final String ConnectionsMaxIdleMsDoc =
        "Idle connections timeout: the server socket processor threads close the connections that"
            + " idle more than this";

    static {
        CONFIG = new ConfigDef().define(MessageMaxBytesProp, ConfigDef.Type.INT, MessageMaxBytes,
            ConfigDef.Range.atLeast(0), ConfigDef.Importance.HIGH, MessageMaxBytesDoc)
            .define(NumNetworkThreadsProp, ConfigDef.Type.INT, NumNetworkThreads,
                ConfigDef.Range.atLeast(1), ConfigDef.Importance.HIGH, NumNetworkThreadsDoc)
            .define(NumIoThreadsProp, ConfigDef.Type.INT, NumIoThreads, ConfigDef.Range.atLeast(1),
                ConfigDef.Importance.HIGH, NumIoThreadsDoc)
            .define(BackgroundThreadsProp, ConfigDef.Type.INT, BackgroundThreads,
                ConfigDef.Range.atLeast(1), ConfigDef.Importance.HIGH, BackgroundThreadsDoc)
            .define(QueuedMaxRequestsProp, ConfigDef.Type.INT, QueuedMaxRequests,
                ConfigDef.Range.atLeast(1), ConfigDef.Importance.HIGH, QueuedMaxRequestsDoc)
            .define(RequestTimeoutMsProp, ConfigDef.Type.INT, RequestTimeoutMs,
                ConfigDef.Importance.HIGH, RequestTimeoutMsDoc)
            .define(ListenersProp, ConfigDef.Type.LIST, ConfigDef.Importance.HIGH,
                ListenersDoc)
            .define(AdvertisedListenersProp, ConfigDef.Type.STRING, null, ConfigDef.Importance.HIGH,
                AdvertisedListenersDoc)
            .define(SocketSendBufferBytesProp, ConfigDef.Type.INT, SocketSendBufferBytes,
                ConfigDef.Importance.HIGH, SocketSendBufferBytesDoc)
            .define(SocketReceiveBufferBytesProp, ConfigDef.Type.INT, SocketReceiveBufferBytes,
                ConfigDef.Importance.HIGH, SocketReceiveBufferBytesDoc)
            .define(SocketRequestMaxBytesProp, ConfigDef.Type.INT, SocketRequestMaxBytes,
                ConfigDef.Range.atLeast(1), ConfigDef.Importance.HIGH, SocketRequestMaxBytesDoc)
            .define(MaxConnectionsPerIpProp, ConfigDef.Type.INT, MaxConnectionsPerIp,
                ConfigDef.Range.atLeast(1), ConfigDef.Importance.MEDIUM, MaxConnectionsPerIpDoc)
            .define(MaxConnectionsPerIpOverridesProp, ConfigDef.Type.STRING,
                MaxConnectionsPerIpOverrides, ConfigDef.Importance.MEDIUM,
                MaxConnectionsPerIpOverridesDoc)
            .define(ConnectionsMaxIdleMsProp, ConfigDef.Type.LONG, ConnectionsMaxIdleMs,
                ConfigDef.Importance.MEDIUM, ConnectionsMaxIdleMsDoc);
    }

    public ProxyServerConfig(Map<?, ?> props) {
        super(CONFIG, props, true);
    }

    public static void main(String[] args) {
        System.out.println(CONFIG.toHtmlTable());
    }

}
