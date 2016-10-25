package io.omega;

import java.util.regex.Pattern;

/**
 * Created by sumit on 10/24/16.
 */
public class BrokerEndpoint {



        private Pattern uriParseExp = Pattern.compile("\[?([0-9a-zA-Z\-%._:]*)\]?:([0-9]+)");

        /**
         * BrokerEndPoint URI is host:port or [ipv6_host]:port
         * Note that unlike EndPoint (or listener) this URI has no security information.
         */
        public  parseHostPort(connectionString: String): Option[(String, Int)] = {
            connectionString match {
                case uriParseExp(host, port) => try Some(host, port.toInt) catch { case e: NumberFormatException => None }
                case _ => None
            }
        }

        /**
         * BrokerEndPoint URI is host:port or [ipv6_host]:port
         * Note that unlike EndPoint (or listener) this URI has no security information.
         */
        def BrokerEndPoint createBrokerEndPoint(brokerId: Int, connectionString: String): BrokerEndPoint = {
                parseHostPort(connectionString).map { case (host, port) => new BrokerEndPoint(brokerId, host, port) }.getOrElse {
            throw new KafkaException("Unable to parse " + connectionString + " to a broker endpoint")
        }
  }

        def readFrom(buffer: ByteBuffer): BrokerEndPoint = {
                val brokerId = buffer.getInt()
                val host = readShortString(buffer)
                val port = buffer.getInt()
                BrokerEndPoint(brokerId, host, port)
        }
    }

/**
 * BrokerEndpoint is used to connect to specific host:port pair.
 * It is typically used by clients (or brokers when connecting to other brokers)
 * and contains no information about the security protocol used on the connection.
 * Clients should know which security protocol to use from configuration.
 * This allows us to keep the wire protocol with the clients unchanged where the protocol is not needed.
 */
    public  BrokerEndPoint(id: Int, host: String, port: Int) {
    }

        public String  connectionString() {
            formatAddress(host, port);
        }

        def writeTo(buffer: ByteBuffer): Unit = {
                buffer.putInt(id)
                writeShortString(buffer, host)
                buffer.putInt(port)
        }

        def sizeInBytes: Int =
                4 + /* broker Id */
                        4 + /* port */
                        shortStringLength(host)
    }

}
