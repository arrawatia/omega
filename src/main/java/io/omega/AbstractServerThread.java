package io.omega;

import org.apache.kafka.common.network.KafkaChannel;
import org.apache.kafka.common.network.Selector;
import org.apache.kafka.common.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;
import java.nio.channels.SocketChannel;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;

public abstract class AbstractServerThread implements Runnable {

    private static final Logger log = LoggerFactory.getLogger(AbstractServerThread.class);

    protected ConnectionQuotas connectionQuotas;
    private CountDownLatch startupLatch = new CountDownLatch(1);
    private CountDownLatch shutdownLatch = new CountDownLatch(1);
    private AtomicBoolean alive = new AtomicBoolean(true);

    public abstract void wakeup() ;

    /**
     * Initiates a graceful shutdown by signaling to stop and waiting for the shutdown to complete
     */
    public void shutdown() {
        alive.set(false);
        wakeup();
        try {
            shutdownLatch.await();
        } catch (InterruptedException e) {
            log.error("{}", e);
        }
    }

    /**
     * Wait for the thread to completely start up
     */
    public void awaitStartup() {
        try {
            startupLatch.await();
        } catch (InterruptedException e) {
            log.error("{}", e);
        }
    }

    /**
     * Record that the thread startup is complete
     */
    protected void startupComplete() {
        startupLatch.countDown();
    }

    /**
     * Record that the thread shutdown is complete
     */
    protected void shutdownComplete() {
        shutdownLatch.countDown();
    }

    /**
     * Is the server still running?
     */
    protected boolean isRunning() {
        return alive.get();
    }

    /**
     * Close the connection identified by `connectionId` and decrement the connection count.
     */
    public void close(Selector selector, String connectionId) {
        KafkaChannel channel = selector.channel(connectionId);
        if (channel != null) {
        log.debug("Closing selector connection " + connectionId);
            InetAddress address = channel.socketAddress();
            if (address != null)
                connectionQuotas.dec(address);
            selector.close(connectionId);
        }
    }

    /**
     * Close `channel` and decrement the connection count.
     */
    public void close(SocketChannel channel) {
        if (channel != null) {
            log.debug("Closing connection from " + channel.socket().getRemoteSocketAddress());
            connectionQuotas.dec(channel.socket().getInetAddress());
            Utils.closeQuietly(channel.socket(), "Error while closing socket for " + channel.socket().getRemoteSocketAddress());
            Utils.closeQuietly(channel, "Error while closing channel ");
        }
    }
}
