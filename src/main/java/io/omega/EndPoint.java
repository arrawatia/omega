package io.omega;

import org.apache.kafka.common.protocol.SecurityProtocol;

import java.nio.ByteBuffer;

/**
 * Created by sumit on 10/24/16.
 */
public class EndPoint {


    private final String host;
    private final int port;

    public String host() {
        return host;
    }

    public int port() {
        return port;
    }

    public SecurityProtocol protocolType() {
        return protocolType;
    }

    private final SecurityProtocol protocolType;

    public EndPoint(String host, int port, SecurityProtocol protocolType) {
        this.host= host;
        this.port=port;
        this.protocolType=protocolType;

    }
//    private String uriParseExp = "^(.*)://\[?([0-9a-zA-Z\-%._:]*)\]?:(-?[0-9]+)";

    public EndPoint readFrom(ByteBuffer buffer) {


//        val port = buffer.getInt()
//        val host = readShortString(buffer)
//        val protocol = buffer.getShort()
//        EndPoint(host, port, SecurityProtocol.forId(protocol))

        return null;
    }

    /**
     * Create EndPoint object from connectionString
     *
     * @param connectionString the format is protocol://host:port or protocol://[ipv6 host]:port
     *                         for example: PLAINTEXT://myhost:9092 or PLAINTEXT://[::1]:9092
     *                         Host can be empty (PLAINTEXT://:9092) in which case we'll bind to default interface
     *                         Negative ports are also accepted, since they are used in some unit tests
     */
    public EndPoint createEndPoint(String connectionString) {


//        connectionString match {
//        case uriParseExp(protocol, "", port) =>new EndPoint(null, port.toInt, SecurityProtocol.forName(protocol))
//        case uriParseExp(protocol, host, port) =>new EndPoint(host, port.toInt, SecurityProtocol.forName(protocol))
//        case _ =>throw new KafkaException("Unable to parse " + connectionString + " to a broker endpoint")
        return null;
    }


    /**
     * Part of the broker definition - matching host/port pair to a protocol
     */


    public String connectionString() {
//            val hostport =
//        if (host == null)
//            ":" + port
//        else
//            Utils.formatAddress(host, port)
//        protocolType + "://" + hostport
        return null;
    }

    public void writeTo(ByteBuffer buffer) {
//            buffer.putInt(port)
//            writeShortString(buffer, host)
//            buffer.putShort(protocolType.id)
    }

    public int sizeInBytes() {
//            4 + /* port */
//                    shortStringLength(host) +
//                    2 /* protocol id */
        return -1;
    }

}