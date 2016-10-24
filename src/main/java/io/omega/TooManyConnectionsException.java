package io.omega;

import java.net.InetAddress;

public class TooManyConnectionsException extends RuntimeException {
    public TooManyConnectionsException(InetAddress ip, Integer count) {
        super("Too many connections from %s (maximum = %d)".format(ip.toString(), count));
    }
}

