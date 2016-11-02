package io.omega.proxy;

import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.requests.MetadataResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;

public class MetadataCache {
    private static final Logger log = LoggerFactory.getLogger(MetadataCache.class);

    public HashMap<TopicPartition, Node> topicLeaderMetadataCache = new HashMap<>();

    public void update(List<MetadataResponse.TopicMetadata> metadata) {
        for (MetadataResponse.TopicMetadata m : metadata) {
            String topic = m.topic();
            m.partitionMetadata().stream().forEach(p ->
                    topicLeaderMetadataCache.put(new TopicPartition(topic, p.partition()), p.leader())
            );
        }
        log.trace("Metadata cache : {} ", this.topicLeaderMetadataCache);
    }

    public Node getLeaderForTopicPartition(TopicPartition tp){
        return topicLeaderMetadataCache.getOrDefault(tp, null);
    }
}
