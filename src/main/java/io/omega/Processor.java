package io.omega;

import org.apache.kafka.common.protocol.SecurityProtocol;
import org.apache.kafka.common.utils.Time;

import java.nio.channels.SocketChannel;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * Created by arrawatia on 10/3/16.
 */
public class Processor extends AbstractServerThread {

 public Processor(int id,
    Time time,
    int maxRequestSize,
     RequestChannel requestChannel,
     ConnectionQuotas connectionQuotas ,
     Long connectionsMaxIdleMs ,
     SecurityProtocol protocol ,
    Map <String, ?> channelConfigs,
     MetricsService metrics )  {}


private Queue newConnections = new ConcurrentLinkedQueue<SocketChannel>()
private val inflightResponses = new HashMap<String, RequestChannel.Response>()
private val metricTags = Map("networkProcessor" -> id.toString).asJava

//    newGauge("IdlePercent",
    //    new Gauge[Double] {
    //    def value = {
    //    metrics.metrics().get(metrics.metricName("io-wait-ratio", "socket-server-metrics",
    // metricTags)).value()
    //    }
    //    },
    //    metricTags.asScala
    //    )

private val selector = new KSelector(
    maxRequestSize,
    connectionsMaxIdleMs,
    metrics,
    time,
    "socket-server",
    metricTags,
    false,
    ChannelBuilders.create(protocol, Mode.SERVER, LoginType.SERVER, channelConfigs, null, true))

    override def run() {
    startupComplete()
    while (isRunning) {
    try {
    // setup any new connections that have been queued up
    configureNewConnections()
    // register any new responses for writing
    processNewResponses()
    poll()
    processCompletedReceives()
    processCompletedSends()
    processDisconnected()
    } catch {
    // We catch all the throwables here to prevent the processor thread from exiting. We do this because
    // letting a processor exit might cause a bigger impact on the broker. Usually the exceptions thrown would
    // be either associated with a specific socket channel or a bad request. We just ignore the bad socket channel
    // or request. This behavior might need to be reviewed if we see an exception that need the entire broker to stop.
    case e: ControlThrowable => throw e
    case e: Throwable =>
    error("Processor got uncaught exception.", e)
    }
    }

    debug("Closing selector - processor " + id)
    swallowError(closeAll())
    shutdownComplete()
    }

private def processNewResponses() {
    var curr = requestChannel.receiveResponse(id)
    while (curr != null) {
    try {
    curr.responseAction match {
    case RequestChannel.NoOpAction =>
    // There is no response to send to the client, we need to read more pipelined requests
    // that are sitting in the server's socket buffer
    curr.request.updateRequestMetrics
    trace("Socket server received empty response to send, registering for read: " + curr)
    selector.unmute(curr.request.connectionId)
    case RequestChannel.SendAction =>
    sendResponse(curr)
    case RequestChannel.CloseConnectionAction =>
    curr.request.updateRequestMetrics
    trace("Closing socket connection actively according to the response code.")
    close(selector, curr.request.connectionId)
    }
    } finally {
    curr = requestChannel.receiveResponse(id)
    }
    }
    }

  /* `protected` for test usage */
protected[network] def sendResponse(response: RequestChannel.Response) {
    trace(s"Socket server received response to send, registering for write and sending data: $response")
    val channel = selector.channel(response.responseSend.destination)
    // `channel` can be null if the selector closed the connection because it was idle for too long
    if (channel == null) {
    warn(s"Attempting to send response via channel for which there is no open connection, connection id $id")
    response.request.updateRequestMetrics()
    }
    else {
    selector.send(response.responseSend)
    inflightResponses += (response.request.connectionId -> response)
    }
    }

private def poll() {
    try selector.poll(300)
    catch {
    case e @ (_: IllegalStateException | _: IOException) =>
    error(s"Closing processor $id due to illegal state or IO exception")
    swallow(closeAll())
    shutdownComplete()
    throw e
    }
    }

private def processCompletedReceives() {
    selector.completedReceives.asScala.foreach { receive =>
    try {
    val channel = selector.channel(receive.source)
    val session = RequestChannel.Session(new KafkaPrincipal(KafkaPrincipal.USER_TYPE, channel.principal.getName),
    channel.socketAddress)
    val req = RequestChannel.Request(processor = id, connectionId = receive.source, session = session, buffer = receive.payload, startTimeMs = time.milliseconds, securityProtocol = protocol)
    requestChannel.sendRequest(req)
    selector.mute(receive.source)
    } catch {
    case e @ (_: InvalidRequestException | _: SchemaException) =>
    // note that even though we got an exception, we can assume that receive.source is valid. Issues with constructing a valid receive object were handled earlier
    error(s"Closing socket for ${receive.source} because of error", e)
    close(selector, receive.source)
    }
    }
    }

private def processCompletedSends() {
    selector.completedSends.asScala.foreach { send =>
    val resp = inflightResponses.remove(send.destination).getOrElse {
    throw new IllegalStateException(s"Send for ${send.destination} completed, but not in `inflightResponses`")
    }
    resp.request.updateRequestMetrics()
    selector.unmute(send.destination)
    }
    }

private def processDisconnected() {
    selector.disconnected.asScala.foreach { connectionId =>
    val remoteHost = ConnectionId.fromString(connectionId).getOrElse {
    throw new IllegalStateException(s"connectionId has unexpected format: $connectionId")
    }.remoteHost
    inflightResponses.remove(connectionId).foreach(_.request.updateRequestMetrics())
    // the channel has been closed by the selector but the quotas still need to be updated
    connectionQuotas.dec(InetAddress.getByName(remoteHost))
    }
    }

    /**
     * Queue up a new connection for reading
     */
    def accept(socketChannel: SocketChannel) {
    newConnections.add(socketChannel)
    wakeup()
    }

/**
 * Register any new connections that have been queued up
 */
private def configureNewConnections() {
    while (!newConnections.isEmpty) {
    val channel = newConnections.poll()
    try {
    debug(s"Processor $id listening to new connection from ${channel.socket.getRemoteSocketAddress}")
    val localHost = channel.socket().getLocalAddress.getHostAddress
    val localPort = channel.socket().getLocalPort
    val remoteHost = channel.socket().getInetAddress.getHostAddress
    val remotePort = channel.socket().getPort
    val connectionId = ConnectionId(localHost, localPort, remoteHost, remotePort).toString
    selector.register(connectionId, channel)
    } catch {
    // We explicitly catch all non fatal exceptions and close the socket to avoid a socket leak. The other
    // throwables will be caught in processor and logged as uncaught exceptions.
    case NonFatal(e) =>
    // need to close the channel here to avoid a socket leak.
    close(channel)
    error(s"Processor $id closed connection from ${channel.getRemoteAddress}", e)
    }
    }
    }

/**
 * Close the selector and all open connections
 */
private def closeAll() {
    selector.channels.asScala.foreach { channel =>
    close(selector, channel.id)
    }
    selector.close()
    }

  /* For test usage */
private[network] def channel(connectionId: String): Option[KafkaChannel] =
    Option(selector.channel(connectionId))

/**
 * Wakeup the thread for selection.
 */
@Override
def wakeup = selector.wakeup()

    }

