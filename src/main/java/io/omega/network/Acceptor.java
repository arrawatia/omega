package io.omega.network;

import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.network.Selectable;
import org.apache.kafka.common.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.SocketException;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.Iterator;
import java.util.Set;

import io.omega.server.EndPoint;

public class Acceptor extends AbstractServerThread {

    private static final Logger log = LoggerFactory.getLogger(Acceptor.class);

    private final Selector nioSelector;
    private final int recvBufferSize;
    private final int sendBufferSize;
    private final Processor[] processors;
    ServerSocketChannel serverChannel;

    public Acceptor(EndPoint endPoint, int sendBufferSize, int recvBufferSize, int brokerId, Processor[] processors, ConnectionQuotas connectionQuotas) throws IOException {

        this.nioSelector = Selector.open();
        this.sendBufferSize = sendBufferSize;
        log.debug("Acceptor : recvBufferSize = {}", recvBufferSize);
        this.recvBufferSize = recvBufferSize;
        this.processors = processors;
        this.connectionQuotas = connectionQuotas;
        this.serverChannel = openServerSocket(endPoint.host(), endPoint.port());
        synchronized (this) {
            for (Processor processor : processors) {
                Utils.newThread("kafka-network-thread-%d-%s-%d".format("" + brokerId, endPoint.protocolType().toString(), processor.id()), processor, false).start();
            }
        }
    }

    /**
     * Accept loop that checks for new connection attempts
     */
    public void run() {
        try {
            serverChannel.register(nioSelector, SelectionKey.OP_ACCEPT);
            startupComplete();
            int currentProcessor = 0;
            while (isRunning()) {
                try {
                    int ready = nioSelector.select(500);
                    if (ready > 0) {
                        Set<SelectionKey> keys = nioSelector.selectedKeys();
                        Iterator<SelectionKey> iter = keys.iterator();
                        while (iter.hasNext() && isRunning()) {
                            try {
                                SelectionKey key = iter.next();
                                iter.remove();
                                if (key.isAcceptable())
                                    accept(key, processors[currentProcessor]);
                                else
                                    throw new IllegalStateException("Unrecognized key state for acceptor thread.");

                                // round robin to the next processor thread
                                currentProcessor = (currentProcessor + 1) % processors.length;
                            } catch (Exception e) {
                                log.error("Error while accepting connection {}", e);
                            }
                        }
                    }
                } catch (Throwable e) {
                    // We catch all the throwables to prevent the acceptor thread from exiting on exceptions due
                    // to a select operation on a specific channel or a bad request. We don't want
                    // the broker to stop responding to requests from other clients in these scenarios.
                    log.error("Error occurred {}", e);
                }
            }
        } catch (ClosedChannelException e) {
            log.error("Error occurred {}", e);
        } finally {
            log.debug("Closing server socket and selector.");
            Utils.closeQuietly(serverChannel, "Error while closing channel " + serverChannel);
            Utils.closeQuietly(nioSelector, "Error while closing selector " + nioSelector);
            shutdownComplete();
        }
    }

    /*
     * Create a server socket to listen for connections on.
     */
    private ServerSocketChannel openServerSocket(String host, int port) throws IOException {
        InetSocketAddress socketAddress = (host == null || host.trim().isEmpty()) ? new InetSocketAddress(port) : new InetSocketAddress(host, port);
        serverChannel = ServerSocketChannel.open();
        serverChannel.configureBlocking(false);
        if (recvBufferSize != Selectable.USE_DEFAULT_BUFFER_SIZE)
            serverChannel.socket().setReceiveBufferSize(recvBufferSize);

        try {
            serverChannel.socket().bind(socketAddress);
            log.info("Awaiting socket connections on {}:{}.",socketAddress.getHostString(), serverChannel.socket().getLocalPort());
        } catch (SocketException e) {
            throw new KafkaException("Socket server failed to bind to %s:%d: %s.".format(socketAddress.getHostString(), port, e.getMessage()), e);
        }
        return serverChannel;
    }

    /*
     * Accept a new connection
     */
    public void accept(SelectionKey key, Processor processor) throws IOException {
        SocketChannel socketChannel = null;
        try {
            ServerSocketChannel serverSocketChannel = (ServerSocketChannel) key.channel();
            socketChannel = serverSocketChannel.accept();
            connectionQuotas.inc(socketChannel.socket().getInetAddress());
            socketChannel.configureBlocking(false);
            socketChannel.socket().setTcpNoDelay(true);
            socketChannel.socket().setKeepAlive(true);
            if (sendBufferSize != Selectable.USE_DEFAULT_BUFFER_SIZE)
                socketChannel.socket().setSendBufferSize(sendBufferSize);

            log.debug("Accepted connection from {} on {} and assigned it to processor {}, sendBufferSize [actual|requested]: [{}|{}] recvBufferSize [actual|requested]: [{}|{}]",
                            socketChannel.socket().getRemoteSocketAddress(),
                            socketChannel.socket().getLocalSocketAddress(),
                            processor.id(),
                            socketChannel.socket().getSendBufferSize(),
                            sendBufferSize,
                            socketChannel.socket().getReceiveBufferSize(),
                            recvBufferSize);

            processor.accept(socketChannel);
        } catch (TooManyConnectionsException e) {
            log.info("Rejected connection from {}, address already has the configured maximum of {} connections.", e.ip(), e.count());
            close(socketChannel);
        }
    }

    /**
     * Wakeup the thread for selection.
     */
    @Override
    public void wakeup() {
        nioSelector.wakeup();
    }

}


