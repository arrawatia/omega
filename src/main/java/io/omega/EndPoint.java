package io.omega;

import org.apache.kafka.common.protocol.SecurityProtocol;
import org.apache.kafka.common.utils.Utils;

import java.util.regex.Matcher;
import java.util.regex.Pattern;


public class EndPoint {

    private static final Pattern PROTOCOL_HOST_PORT_PATTERN = Pattern.compile("^(.*)://\\[?([0-9a-zA-Z\\-%._:]*)\\]?:([0-9]+)");
    private final String host;
    private final int port;
    private final SecurityProtocol protocolType;

    public EndPoint(String host, int port, SecurityProtocol protocolType) {
        this.host= host;
        this.port=port;
        this.protocolType=protocolType;
    }

    public String host() {
        return host;
    }

    public int port() {
        return port;
    }

    public SecurityProtocol protocolType() {
        return protocolType;
    }

    /**
     * Create EndPoint object from connectionString
     *
     * @param connectionString the format is protocol://host:port or protocol://[ipv6 host]:port
     *                         for example: PLAINTEXT://myhost:9092 or PLAINTEXT://[::1]:9092
     *                         Host can be empty (PLAINTEXT://:9092) in which case we'll bind to default interface
     *                         Negative ports are also accepted, since they are used in some unit tests
     */
    public static EndPoint createEndPoint(String connectionString) {
        System.out.println(connectionString);
        Matcher matcher = PROTOCOL_HOST_PORT_PATTERN.matcher(connectionString);
        if (matcher.matches()) {
            System.out.println(matcher.group(0));
            System.out.println(matcher.group(1));
            System.out.println(matcher.group(2));
            System.out.println(matcher.group(3));
            SecurityProtocol sp = SecurityProtocol.forName(matcher.group(1));
            String host = matcher.group(2);
            int port = Integer.parseInt(matcher.group(3));
            return new EndPoint(host, port, sp);
        } else {
            return null;
        }
    }

    /**
     * Part of the broker definition - matching host/port pair to a protocol
     */

    public String connectionString() {
        String hostPort;
        if (host == null) {
            hostPort = ":" + port;
        } else {
            hostPort = Utils.formatAddress(host, port);
        }
        return protocolType + "://" + hostPort;
    }
}