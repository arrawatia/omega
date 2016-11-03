import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.Collections;
import java.util.Properties;
import java.util.regex.Pattern;

public class Foo {

    private static final Pattern HOST_PORT_PATTERN = Pattern.compile("^(.*)://\\[?([0-9a-zA-Z\\-%._:]*)\\]?:([0-9]+)");

    public static void main(String... args) {
//        Matcher matcher = HOST_PORT_PATTERN.matcher("PLAINTEXT://0.0.0.0:9090");
//        if (matcher.matches())
//            System.out.println(matcher.group(0));
//        System.out.println(matcher.group(1));
//        System.out.println(matcher.group(2));
//        System.out.println(matcher.group(3));

                consumeTestData(createConsumer(), "bar");

    }

    private static KafkaConsumer<String, String> createConsumer() {
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9088");
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "test-group");
        properties.put(ConsumerConfig.CLIENT_ID_CONFIG, "test-consumer");
        return new KafkaConsumer<>(properties, new StringDeserializer(), new StringDeserializer());
    }

    private static void produceTestData(Producer<String, String> producer, String topic, int numRecords) {
        for (int i = 0; i < numRecords; i++)
            producer.send(new ProducerRecord<>(topic, Integer.toString(i), Integer.toString(i)));
    }

    private static void consumeTestData(KafkaConsumer<String, String> consumer, String topic){
        consumer.subscribe(Collections.singleton(topic));
        while(true){
        ConsumerRecords<String, String> records = consumer.poll(200);
        for (ConsumerRecord<String, String> record : records) {
            System.out.println(record.toString());
        }
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }
}
