package io.omega.proxy;

import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.types.Struct;
import org.apache.kafka.common.requests.GroupCoordinatorRequest;
import org.apache.kafka.common.requests.GroupCoordinatorResponse;
import org.apache.kafka.common.requests.MetadataRequest;
import org.apache.kafka.common.requests.MetadataResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import io.omega.ProxyServerConfig;
import io.omega.client.KafkaProtocolClient;

public class MetadataCache {
    private static final Logger log = LoggerFactory.getLogger(MetadataCache.class);

    private HashMap<TopicPartition, Node> topicLeaderMetadataCache = new HashMap<>();
    private HashMap<String, Node> groupIdToCoordinator = new HashMap<>();
    private Node controller;
    private String clusterId;
    private ArrayList<Node> brokers;
    private final KafkaProtocolClient client;
    private int timeOutInMs = 100000;

    public MetadataCache(ProxyServerConfig cfg) {
            this.client = new KafkaProtocolClient(cfg);
    }

    public void update(MetadataResponse metadata) {

        brokers = new ArrayList<>(metadata.brokers());
        this.clusterId = metadata.clusterId();
        this.controller = metadata.controller();
        List<MetadataResponse.TopicMetadata> topicMetadata = new ArrayList<>(metadata.topicMetadata());
        for (MetadataResponse.TopicMetadata m : topicMetadata) {
            String topic = m.topic();
            m.partitionMetadata().stream().forEach(p ->
                    topicLeaderMetadataCache.put(new TopicPartition(topic, p.partition()), p.leader())
            );
        }
        log.trace("Metadata cache : {} ", this.topicLeaderMetadataCache);
    }

    public Node getLeaderForTopicPartition(TopicPartition tp) {

        Node leader =  topicLeaderMetadataCache.getOrDefault(tp, null);
        if(leader == null) {
            // Try once more !
            fetchMetadata();
            leader =  topicLeaderMetadataCache.getOrDefault(tp, null);
            log.error("Leader not found for topic partition {}. Cache {}", tp, topicLeaderMetadataCache);
        }

        return leader;
    }

    public void fetchMetadata() {
        ArrayList<String> topics = null;
        MetadataRequest request = new MetadataRequest(topics);
        Struct responseBody = client.sendAnyNode(ApiKeys.METADATA, request, timeOutInMs);
        log.trace("Refresh metadata {}", responseBody);
        if (responseBody != null) {
            MetadataResponse response = new MetadataResponse(responseBody);
            update(response);
        }
    }

    public Node controller() {
        if(this.controller == null){
            fetchMetadata();
        }
        return this.controller;
    }

    public void updateCoordinatorForGroup(String groupId, Node coordinator){
        groupIdToCoordinator.put(groupId, coordinator);
    }

    public Node coordinator(String groupId){
        Node coordinator = groupIdToCoordinator.getOrDefault(groupId, null);
        if(coordinator == null){
            fetchCoordinator(groupId);
            coordinator = groupIdToCoordinator.getOrDefault(groupId, null);
        }
        return coordinator;
    }

    private void fetchCoordinator(String groupId) {
        GroupCoordinatorRequest request = new GroupCoordinatorRequest(groupId);
        Struct responseBody = client.sendAnyNode(ApiKeys.GROUP_COORDINATOR, request, 1000000);
        GroupCoordinatorResponse response = new GroupCoordinatorResponse(responseBody);
        groupIdToCoordinator.put(groupId, response.node());
    }
}
