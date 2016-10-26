package io.omega;

import org.apache.kafka.clients.CommonClientConfigs;

import java.util.HashMap;
import java.util.Map;

import io.omega.client.KafkaProtocolClient;

/**
 * Created by sumit on 10/26/16.
 */
public class TestProxy {

    public static void main(String[] args){
        Map<String, String> cfg = new HashMap<>();
        cfg.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, "localhost:9088");
        KafkaProtocolClient client = new KafkaProtocolClient(cfg);
        System.out.print(client.findAllBrokers(1000));

    }
}
