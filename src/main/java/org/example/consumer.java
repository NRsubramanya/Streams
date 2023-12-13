package org.example;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

public class consumer {

    public static void main(String[] args) {

        Properties p = new Properties();

        p.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        p.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        p.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        p.setProperty(ConsumerConfig.GROUP_ID_CONFIG , "consumer-group3");
        p.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        final KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(p);

        consumer.subscribe(Arrays.asList("topic1"));

        while (true) {
            ConsumerRecords<String, String> records =  consumer.poll(Duration.ofMillis(1000));
            for (ConsumerRecord record: records) {
                System.out.println("Received new record: \n" +
                        "Key: " + record.key() + ", " +
                        "Value: " + record.value() + ", " +
                        "Topic: " + record.topic() + ", " +
                        "Partition: " + record.partition() + ", " +
                        "Offset: " + record.offset() + "\n");
//
            }

        }
    }

    public static void subscribe(List<String> list) {
    }
}
