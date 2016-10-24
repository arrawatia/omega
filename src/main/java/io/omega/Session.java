package io.omega;

import org.apache.kafka.common.security.auth.KafkaPrincipal;

import java.net.InetAddress;

public class Session {

    public Session(KafkaPrincipal principal, InetAddress clientAddress) {
//         this.sanitizedUser = QuotaId.sanitize(principal.getName());
    }
}
