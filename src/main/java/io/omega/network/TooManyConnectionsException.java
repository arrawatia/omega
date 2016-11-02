package io.omega.network;

import java.net.InetAddress;

public class TooManyConnectionsException extends RuntimeException {

    private final InetAddress ip;
    private final Integer count;

    public TooManyConnectionsException(InetAddress ip, Integer count) {
        super("Too many connections from %s (maximum = %d)".format(ip.toString(), count));
        this.ip = ip;
        this.count = count;
    }

    public InetAddress ip() {
        return ip;
    }

    public Integer count() {
        return count;
    }
}

