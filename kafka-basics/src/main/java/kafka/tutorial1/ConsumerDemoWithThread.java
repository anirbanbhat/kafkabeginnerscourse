package kafka.tutorial1;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public class ConsumerDemoWithThread {

    public static void main(String[] args) {
        new ConsumerDemoWithThread().run();
    }

    private ConsumerDemoWithThread() {

    }

    private void run() {
        Logger LOGGER = LoggerFactory.getLogger(ConsumerDemoWithThread.class);

        String bootstrapServer = "127.0.0.1:9092";
        String groupId = "my-sixth-application";
        String topic = "first-topic";

        // Latch for dealing with multiple threads
        CountDownLatch latch = new CountDownLatch(1);

        LOGGER.info("Creating Consumer Runnables");
        ConsumerRunnable myConsumerRunnable = new ConsumerRunnable(bootstrapServer, groupId, topic, latch);

        Thread myThread = new Thread(myConsumerRunnable);
        myThread.start();

        Runtime.getRuntime().addShutdownHook(new Thread( () -> {
            LOGGER.info("Caught Shutdown Hook!");
            myConsumerRunnable.shutDown();
            try {
                latch.wait();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }));

        try {
            latch.wait();
        } catch (InterruptedException e) {
            LOGGER.info("Application got interrupted!");
        } finally {
            LOGGER.info("Application is closing!");
        }
    }

    public class ConsumerRunnable implements Runnable {

        Logger LOGGER = LoggerFactory.getLogger(ConsumerRunnable.class);
        private CountDownLatch latch;
        KafkaConsumer<String, String> consumer;

        public ConsumerRunnable(String bootstrapServer,
                                String groupId,
                                String topic,
                                CountDownLatch latch) {
            this.latch = latch;
            // Create consumer config
            Properties properties = new Properties();
            properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
            properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
            properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

            // Create consumer
            consumer = new KafkaConsumer<>(properties);

            // Subscribe to our topic(S)
            consumer.subscribe(Arrays.asList(topic)); // consumer.subscribe(Collictions.singleton("first-topic"));
        }

        @Override
        public void run() {
            try {
                // Poll for new data
                while(true){
                    ConsumerRecords<String, String> records =
                            consumer.poll(Duration.ofMillis(100)); // new in Kafka 2.0
                    for (ConsumerRecord<String, String> record: records){
                        LOGGER.info("Key: " + record.key() + "\nValue: " + record.value());
                        LOGGER.info("Topic: " + record.topic() + "\nOffset: " + record.offset());
                    }
                }
            } catch (WakeupException e) {
                LOGGER.info("Received shutdown signal!");
            } finally {
                consumer.close();
                latch.countDown();
            }
        }

        public void shutDown() {
            // a special method to interrupt consumer.poll();
            consumer.wakeup();
        }
    }
}
