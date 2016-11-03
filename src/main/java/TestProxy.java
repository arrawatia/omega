import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.protocol.types.Struct;
import org.apache.kafka.common.requests.GroupCoordinatorRequest;
import org.apache.kafka.common.requests.GroupCoordinatorResponse;

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

        GroupCoordinatorRequest request = new GroupCoordinatorRequest("foo");
        Struct responseBody = client.sendAnyNode(ApiKeys.GROUP_COORDINATOR, request, 100000);
        GroupCoordinatorResponse response = new GroupCoordinatorResponse(responseBody);
        Errors.forCode(response.errorCode()).maybeThrow();
        System.out.print(response.node());

    }
}
