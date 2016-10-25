package io.omega;

import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.network.Selectable;
import org.apache.kafka.common.utils.Utils;

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

public class Acceptor extends AbstractServerThread {

    private final Selector nioSelector;
    private final Integer recvBufferSize;
    private final int sendBufferSize;
    private final Processor[] processors;
    ServerSocketChannel serverChannel;

    public Acceptor(EndPoint endPoint, int sendBufferSize, Integer recvBufferSize, int brokerId, Processor[] processors, ConnectionQuotas connectionQuotas) throws IOException {

        this.nioSelector = Selector.open();
        this.serverChannel = openServerSocket(endPoint.host(), endPoint.port());
        this.sendBufferSize = sendBufferSize;
        this.recvBufferSize = recvBufferSize;
        this.processors = processors;
        this.connectionQuotas = connectionQuotas;
        synchronized (this) {
            for (Processor processor : processors) {
                Utils.newThread("kafka-network-thread-%d-%s-%d".format(""+ brokerId, endPoint.protocolType().toString(), processor.id()), processor, false).start();
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
//                                    error("Error while accepting connection", e);
                            }
                        }
                    }
                } catch (Throwable e){
                    // We catch all the throwables to prevent the acceptor thread from exiting on exceptions due
                    // to a select operation on a specific channel or a bad request. We don't want
                    // the broker to stop responding to requests from other clients in these scenarios.

//                    error("Error occurred", e);
                }
            }
        } catch (ClosedChannelException e) {
            e.printStackTrace();
        } finally {
//            debug("Closing server socket and selector.");
            try {
                serverChannel.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
            try {
                nioSelector.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
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
//    info("Awaiting socket connections on %s:%d.".format(socketAddress.getHostString, serverChannel.socket.getLocalPort))
    } catch (SocketException e) {
    throw new KafkaException("Socket server failed to bind to %s:%d: %s.".format(socketAddress.getHostString(), port, e.getMessage()), e);
    }
     return serverChannel;
    }

  /*
   * Accept a new connection
   */
    public void  accept( SelectionKey key , Processor processor) {
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

//            debug("Accepted connection from %s on %s and assigned it to processor %d, sendBufferSize [actual|requested]: [%d|%d] recvBufferSize [actual|requested]: [%d|%d]"
//                    .format(socketChannel.socket.getRemoteSocketAddress, socketChannel.socket.getLocalSocketAddress, processor.id,
//                            socketChannel.socket.getSendBufferSize, sendBufferSize,
//                            socketChannel.socket.getReceiveBufferSize, recvBufferSize))

            processor.accept(socketChannel);
        } catch (Exception e){
//                info("Rejected connection from %s, address already has the configured maximum of %d connections.".format(e.ip, e.count))
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


