package io.omega;

import org.apache.kafka.common.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ConnectionId {

    private static final Logger log = LoggerFactory.getLogger(ConnectionId.class);

    private final String localHost;
    private final int localPort;
    private final String remoteHost;
    private final int remotePort;

    public ConnectionId(String localHost, int localPort, String remoteHost, int remotePort) {
        this.localHost = localHost;
        this.localPort = localPort;
        this.remoteHost = remoteHost;
        this.remotePort = remotePort;
    }

    public static ConnectionId fromString(String s) {
        String[] hostPorts = s.split("-");
        log.debug("ConnectionId > fromString {}", s);
        String localHostPort = hostPorts[0];
        String localHost = Utils.getHost(localHostPort);
        int localPort = Utils.getPort(localHostPort);
        String remoteHostPort = hostPorts[1];
        String remoteHost = Utils.getHost(remoteHostPort);
        int remotePort = Utils.getPort(remoteHostPort);
        return new ConnectionId(localHost, localPort, remoteHost, remotePort);
    }

    public String localHost() {
        return localHost;
    }

    public int localPort() {
        return localPort;
    }

    public String remoteHost() {
        return remoteHost;
    }

    public int remotePort() {
        return remotePort;
    }

    @Override
    public String toString() {
        return this.localHost + ":" + this.localPort + "-" + this.remoteHost + ":" + this.remotePort;
    }
}
