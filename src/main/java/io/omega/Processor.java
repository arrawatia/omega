package io.omega;

import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.network.ChannelBuilders;
import org.apache.kafka.common.network.KafkaChannel;
import org.apache.kafka.common.network.LoginType;
import org.apache.kafka.common.network.Mode;
import org.apache.kafka.common.network.Selector;
import org.apache.kafka.common.protocol.SecurityProtocol;
import org.apache.kafka.common.security.auth.KafkaPrincipal;
import org.apache.kafka.common.utils.Time;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.channels.SocketChannel;
import java.util.HashMap;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

public class Processor extends AbstractServerThread {


    private SecurityProtocol protocol;
    private Time time;

    public int id() {
        return id;
    }

    private int id;
    private RequestChannel requestChannel;
    private Selector selector;
    private Queue newConnections = new ConcurrentLinkedQueue<SocketChannel>();
    private Map<String, Response> inflightResponses = new HashMap<>();
    private Map<String, String> metricTags = new HashMap<>();

    public Processor(int id,
                     Time time,
                     int maxRequestSize,
                     RequestChannel requestChannel,
                     ConnectionQuotas connectionQuotas,
                     Long connectionsMaxIdleMs,
                     SecurityProtocol protocol,
                     Map<String, ?> channelConfigs,
                     Metrics metrics) {
//        super(connectionQuotas);
        this.connectionQuotas = connectionQuotas;
        this.time = time;
        this.protocol = protocol;
        this.id = id;
        this.requestChannel = requestChannel;
        metricTags.put("networkProcessor", "" + id);
        selector = new Selector(
                maxRequestSize,
                connectionsMaxIdleMs,
                metrics,
                time,
                "socket-server",
                metricTags,
                false,
                ChannelBuilders.create(protocol, Mode.SERVER, LoginType.SERVER, channelConfigs, null, true));

    }


//    newGauge("IdlePercent",
    //    new Gauge[Double] {
    //    def value = {
    //    metrics.metrics().get(metrics.metricName("io-wait-ratio", "socket-server-metrics",
    // metricTags)).value()
    //    }
    //    },
    //    metricTags.asScala
    //    )

    @Override
    public void run() {
        startupComplete();
        while (isRunning()) {
            try {
                // setup any new connections that have been queued up
                configureNewConnections();
                // register any new responses for writing
                processNewResponses();
                poll();
                processCompletedReceives();
                processCompletedSends();
                processDisconnected();
            } catch (Throwable e) {
                // We catch all the throwables here to prevent the processor thread from exiting. We do this because
                // letting a processor exit might cause a bigger impact on the broker. Usually the exceptions thrown would
                // be either associated with a specific socket channel or a bad request. We just ignore the  bad socket channel
                // or request. This behavior might need to be reviewed if we see an exception that need the entire broker to stop.
//                error("Processor got uncaught exception.", e)
//                debug("Closing selector - processor " + id)
                closeAll();
                shutdownComplete();
            }
        }
    }

    private void processNewResponses() {
        Response curr = requestChannel.receiveResponse(this.id);
        while (curr != null) {
        System.out.println("processNewResponses : " + curr);
            try {
                switch (curr.responseAction()) {
                    case NOOP:
                        // There is no response to send to the client, we need to read more
                        // pipelined requests
                        // that are sitting in the server's socket buffer
                        //    curr.request.updateRequestMetrics;
                        //    trace("Socket server received empty response to send, registering
                        // for read: " + curr)
                        selector.unmute(curr.request().connectionId());
                        break;
                    case SEND:
                        sendResponse(curr);
                        break;
                    case CLOSE:
//    curr.request.updateRequestMetrics
//    trace("Closing socket connection actively according to the response code.")
                        close(selector, curr.request().connectionId());
                }
            } finally {
                curr = requestChannel.receiveResponse(id);
            }

        }
    }

  /* `protected` for test usage */

    protected void sendResponse(Response response) {
//            trace(s"Socket server received response to send, registering for write and " + "sending data: $response")
        System.out.println("sendResponse : " + response);
        System.out.println("sendResponse > response.responseSend().destination(): " + response.responseSend().destination());


        KafkaChannel channel = selector.channel(response.responseSend().destination());
        System.out.println("sendResponse channel: " + channel);
        // `channel` can be null if the selector closed the connection because it was
        // idle for too long
        if (channel == null) {
//            warn(s"Attempting to send response via channel for which there is no open " +
//                    "connection, connection id $id")
//            response.request().updateRequestMetrics();
        } else {
            selector.send(response.responseSend());
            inflightResponses.put(response.request().connectionId(), response);
        }
    }

    private void poll() throws IOException {
        try {
            selector.poll(300);
        } catch (IllegalStateException | IOException e) {
//                error(s"Closing processor $id due to illegal state or IO exception")
            closeAll();
            shutdownComplete();
            throw e;

        }
    }

    private void processCompletedReceives() {
        selector.completedReceives().stream().forEach(
                receive -> {
                    System.out.println("processCompletedReceives :" + receive);
                    try {
                        KafkaChannel channel = selector.channel(receive.source());
                        Session session = new Session(new KafkaPrincipal(KafkaPrincipal.USER_TYPE, channel.principal().getName()), channel.socketAddress());
                        Request req = new Request(id, receive.source(), session, receive.payload(), time.milliseconds(), protocol);
                        requestChannel.sendRequest(req);
                        selector.mute(receive.source());
                    } catch (Exception e) {
                        // note that even though we got an exception, we can assume that receive
                        // .source is valid. Issues with constructing a valid receive object were
                        // handled earlier
//                    error(s"Closing socket for ${receive.source} because of error", e);
                        close(selector, receive.source());
                    }
                });
    }

    private void processCompletedSends() {
        selector.completedSends().stream().forEach(
                send -> {
                    System.out.println("processCompletedSends :" + send);

                    if (!inflightResponses.containsKey(send.destination())) {
                        throw new IllegalStateException("Send for ${send.destination} completed, but not in `inflightResponses`");
                    }
                    Response resp = inflightResponses.get(send.destination());
                    inflightResponses.remove(send.destination());

//            resp.request()).updateRequestMetrics();
                    selector.unmute(send.destination());
                });
    }

    private void processDisconnected() {
        selector.disconnected().stream().forEach(
                (String connectionId) -> {
                    String remoteHost = ConnectionId.fromString(connectionId).remoteHost();
                    if (remoteHost == null)
                        throw new IllegalStateException("connectionId has unexpected format: " + connectionId);

                    Response r = inflightResponses.remove(connectionId);
//                r.request().updateRequestMetrics();
                    // the channel has been closed by the selector but the quotas still need to be updated
                    try {
                        connectionQuotas.dec(InetAddress.getByName(remoteHost));
                    } catch (UnknownHostException e) {
                        e.printStackTrace();
                    }
                });
    }

    /**
     * Queue up a new connection for reading
     */
    public void accept(SocketChannel socketChannel) {
        newConnections.add(socketChannel);
        wakeup();
    }

    /**
     * Register any new connections that have been queued up
     */
    private void configureNewConnections() {
        while (!newConnections.isEmpty()) {
            SocketChannel channel = (SocketChannel) newConnections.poll();
            try {
//                debug(s"Processor $id listening to new connection from ${channel.socket" +
//                        ".getRemoteSocketAddress}")
                String localHost = channel.socket().getLocalAddress().getHostAddress();
                int localPort = channel.socket().getLocalPort();
                String remoteHost = channel.socket().getInetAddress().getHostAddress();
                int remotePort = channel.socket().getPort();
                String connectionId = new ConnectionId(localHost, localPort, remoteHost, remotePort).toString();
                selector.register(connectionId, channel);
            } catch (Throwable t) {
                // We explicitly catch all non fatal exceptions and close the socket to avoid a
                // socket leak. The other
                // throwables will be caught in processor and logged as uncaught exceptions.
//                case NonFatal(e) =>
                // need to close the channel here to avoid a socket leak.
                close(channel);
//                    error(s"Processor $id closed connection from ${channel.getRemoteAddress}", e)
            }
        }
    }

    /**
     * Close the selector and all open connections
     */
    private void closeAll() {
        selector.channels().stream().forEach(channel -> close(selector, channel.id()));
        selector.close();
    }

    /* For test usage */
    private KafkaChannel channel(String connectionId) {
        return selector.channel(connectionId);
    }

    /**
     * Wakeup the thread for selection.
     */
    @Override
    public void wakeup() {
        System.out.println("waking up " + id);
        selector.wakeup();
    }

}

