import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.types.Struct;
import org.apache.kafka.common.requests.CreateTopicsRequest;
import org.apache.kafka.common.requests.MetadataRequest;
import org.apache.kafka.common.requests.MetadataResponse;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import io.omega.client.KafkaProtocolClient;

public class TestProxy {

    public static void main(String[] args) {
        Map<String, Object> cfg = new HashMap<>();
        cfg.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, "localhost:9088");
        KafkaProtocolClient client = new KafkaProtocolClient(cfg);
        System.out.print(client.findAllBrokers(1000));

//        GroupCoordinatorRequest request = new GroupCoordinatorRequest("foo");
//        Struct responseBody = client.sendAnyNode(ApiKeys.GROUP_COORDINATOR, request, 100000);
//        GroupCoordinatorResponse response = new GroupCoordinatorResponse(responseBody);
//        Errors.forCode(response.errorCode()).maybeThrow();
//        System.out.print(response.node());
//
        ArrayList<String> t = null;
        MetadataRequest mrequest = new MetadataRequest(t);
        Struct mresp = client.sendAnyNode(ApiKeys.METADATA, mrequest, 100000);
        MetadataResponse metadata = new MetadataResponse(mresp);
        Node controller = metadata.controller();
        System.out.println(metadata);

        Map topics = new HashMap<>();
        topics.put("topic1", new CreateTopicsRequest.TopicDetails(1, (short) 1));
        Integer timeout = 10000;
        CreateTopicsRequest topic1 = new CreateTopicsRequest(topics, timeout);
        Struct topic1responseBody = client.sendSync(controller, ApiKeys.CREATE_TOPICS, topic1, 100000);
        System.out.println(topic1responseBody);


    }
}
