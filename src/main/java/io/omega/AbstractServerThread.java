package io.omega;

import org.apache.kafka.common.network.KafkaChannel;
import org.apache.kafka.common.network.Selector;

import java.io.IOException;
import java.net.InetAddress;
import java.nio.channels.SocketChannel;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;

public abstract class AbstractServerThread implements Runnable {

    protected ConnectionQuotas connectionQuotas;
    private CountDownLatch startupLatch = new CountDownLatch(1);
    private CountDownLatch shutdownLatch = new CountDownLatch(1);
    private AtomicBoolean alive = new AtomicBoolean(true);


    public void wakeup() {

    }

    /**
     * Initiates a graceful shutdown by signaling to stop and waiting for the shutdown to complete
     */
    public void shutdown() {
        alive.set(false);
        wakeup();
        try {
            shutdownLatch.await();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    /**
     * Wait for the thread to completely start up
     */
    public void awaitStartup() {
        try {
            startupLatch.await();
        } catch (InterruptedException e) {
            e.printStackTrace();
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
    protected void isRunning() {
        alive.get();
    }

    /**
     * Close the connection identified by `connectionId` and decrement the connection count.
     */
    public void close(Selector selector, String connectionId) {
        KafkaChannel channel = selector.channel(connectionId);
        if (channel != null) {
//    debug(s"Closing selector connection $connectionId")
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
//    debug("Closing connection from " + channel.socket.getRemoteSocketAddress())
            connectionQuotas.dec(channel.socket().getInetAddress());
            try {
                channel.socket().close();
            } catch (IOException e) {
                e.printStackTrace();
            }
            try {
                channel.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }
}
