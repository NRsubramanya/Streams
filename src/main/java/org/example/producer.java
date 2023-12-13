package org.example;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class producer {

    public static void main(String[] args) {

        Properties prop = new Properties();

        prop.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"localhost:9092");
        prop.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        prop.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        final KafkaProducer<String, String> producer = new KafkaProducer<String, String>(prop);

        System.out.println("start");
        int fact=1;
        for (int i=1;i<=10;i++){
            fact=fact*i;
            ProducerRecord<String, String> record = new ProducerRecord<>("topic1" , Integer.toString(i) , Integer.toString(fact));
            producer.send(record);
        }

        producer.flush();
        System.out.println("flushed");
        producer.close();
    }
}
