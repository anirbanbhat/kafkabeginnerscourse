package kafka.tutorial1;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class ConsumerDemo {

    public static void main(String[] args) {
        Logger LOGGER = LoggerFactory.getLogger(ConsumerDemo.class);
        String bootstrapServer = "127.0.0.1:9092";
        String groupId = "my-fourth-application";
        String offsetResetConfig = "earliest";
        // Create consumer config
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, offsetResetConfig);
        // Create consumer
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);
        // Subscribe to our topic(S)
        consumer.subscribe(Arrays.asList("first-topic")); // consumer.subscribe(Collictions.singleton("first-topic"));
        // Poll for new data
        while(true){
            ConsumerRecords<String, String> records =
                    consumer.poll(Duration.ofMillis(100)); // new in Kafka 2.0
            for (ConsumerRecord<String, String> record: records){
                LOGGER.info("Key: " + record.key() + "\nValue: " + record.value());
                LOGGER.info("Topic: " + record.topic() + "\nOffset: " + record.offset());
            }
        }
    }
}
