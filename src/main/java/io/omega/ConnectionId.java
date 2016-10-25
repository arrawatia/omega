package io.omega;

/**
 * Created by sumit on 10/24/16.
 */
public class ConnectionId {


        public static ConnectionId fromString(String s){
            String[] tmp = s.split("-");

            //case Array(local, remote) =>

//            BrokerEndPoint.parseHostPort(local).flatMap { case (localHost, localPort) =>
//                BrokerEndPoint.parseHostPort(remote).map { case (remoteHost, remotePort) =>
//                ConnectionId(localHost, localPort, remoteHost, remotePort)
            return null;

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

    String localHost;
    int localPort;
    String remoteHost;
    int remotePort;

    public ConnectionId(String localHost, int localPort, String remoteHost, int remotePort) {
        this.localHost = localHost;
        this.localPort = localPort;
        this.remoteHost = remoteHost;
        this.remotePort = remotePort;
    }


    @Override
        public String toString() {
            return this.localHost + this.localPort + "-" + this.remoteHost + this.remotePort;
        }
}
