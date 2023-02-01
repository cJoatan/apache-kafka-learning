package org.example;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class ConsumerDemo {
    private static final Logger log = LoggerFactory.getLogger(ConsumerDemo.class.getSimpleName());


    public static void main(String[] args) {
        final String bootstrapServer = "localhost:9092";
        final String groupId = "my-third-application";
        final String topic = "demo_java";

        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");


        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(properties);


        final Thread mainThread = Thread.currentThread();

        Runtime.getRuntime().addShutdownHook(new Thread(){
            public void run() {
                System.out.println("Detected shutdown");
                consumer.wakeup();

                try {
                    mainThread.join();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        });

        try {
            consumer.subscribe(Collections.singletonList(topic));

            while (true) {
                System.out.println("Polling");
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));

                records.forEach(record -> {
                    System.out.println("Key: " + record.key() + " Value: " + record.value());
                    System.out.println("Partition: " + record.partition() + " Offset: " + record.offset());
                });
            }
        } catch(WakeupException wakeupException) {
            log.info("Wake up exception");
            // closing consumer
        } catch (Exception ex) {
            log.error("Unexpected exception", ex);
        } finally {
            consumer.close();
            log.info("The Consumer is now gracefully closed");
        }
    }
}
