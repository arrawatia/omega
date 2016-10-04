package io.omega;

/**
 * Created by arrawatia on 10/3/16.
 */
public class AbstractServerThread {
}


private[kafka] abstract class AbstractServerThread(connectionQuotas: ConnectionQuotas) extends Runnable with Logging {

private val startupLatch = new CountDownLatch(1)
private val shutdownLatch = new CountDownLatch(1)
private val alive = new AtomicBoolean(true)

    def wakeup()

    /**
     * Initiates a graceful shutdown by signaling to stop and waiting for the shutdown to complete
     */
    def shutdown(): Unit = {
    alive.set(false)
    wakeup()
    shutdownLatch.await()
    }

    /**
     * Wait for the thread to completely start up
     */
    def awaitStartup(): Unit = startupLatch.await

/**
 * Record that the thread startup is complete
 */
protected def startupComplete() = {
    startupLatch.countDown()
    }

/**
 * Record that the thread shutdown is complete
 */
protected def shutdownComplete() = shutdownLatch.countDown()

/**
 * Is the server still running?
 */
protected def isRunning = alive.get

    /**
     * Close the connection identified by `connectionId` and decrement the connection count.
     */
    def close(selector: KSelector, connectionId: String) {
    val channel = selector.channel(connectionId)
    if (channel != null) {
    debug(s"Closing selector connection $connectionId")
    val address = channel.socketAddress
    if (address != null)
    connectionQuotas.dec(address)
    selector.close(connectionId)
    }
    }

    /**
     * Close `channel` and decrement the connection count.
     */
    def close(channel: SocketChannel) {
    if (channel != null) {
    debug("Closing connection from " + channel.socket.getRemoteSocketAddress())
    connectionQuotas.dec(channel.socket.getInetAddress)
    swallowError(channel.socket().close())
    swallowError(channel.close())
    }
    }
    }
