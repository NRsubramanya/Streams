package org.example;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;

import java.time.Duration;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.Properties;
import java.util.Random;

public class stream2 {
    public static void main(String[] args) {
        // Set up Kafka producer properties
        Properties producerProps = new Properties();
        producerProps.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        producerProps.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        producerProps.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");

        // Create a Kafka producer
        KafkaProducer<String, String> producer = new KafkaProducer<>(producerProps);

        // Generate and produce 100 key-value pairs to a Kafka topic
        for (int i = 0; i < 10; i++) {
            LocalDate date = LocalDate.now().plusDays(i); // Generate dates in sequence
            String key = date.format(DateTimeFormatter.ofPattern("dd-MM-yyyy"));
            String value = generateRandomString(5); // Generate a random string of length 10
            ProducerRecord<String, String> record = new ProducerRecord<>("lower-topic3", key, value);
            producer.send(record);
        }

        producer.close();

        // Set up Kafka Streams properties
        Properties streamProps = new Properties();
        streamProps.setProperty(StreamsConfig.APPLICATION_ID_CONFIG, "kafka-streams1");
        streamProps.setProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");


        // Build the Kafka Streams topology
        final Serde<String> stringSerde = Serdes.String();
        StreamsBuilder builder = new StreamsBuilder();

        final KStream<String, String> inputStream = builder.stream("lower-topic3", Consumed.with(stringSerde, stringSerde));
        final KStream<String, String> outstream = inputStream.map((key, value) -> new KeyValue<>(key, value.toUpperCase()));
        outstream.to("upper-topic3", Produced.with(Serdes.String(), Serdes.String()));





        KStream<String, String> printStream = builder.stream("upper-topic3");

        printStream.foreach((key, value) -> {
            System.out.println("Received message - Key: " + key + ", Value: " + value);
        });


        Topology topology = builder.build();
        KafkaStreams streams = new KafkaStreams(topology, streamProps);

        // Start the Kafka Streams application
        streams.start();

        // Add shutdown hook to gracefully close the application
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));

        Properties p = new Properties();

        p.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        p.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        p.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        p.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "consumer-group4");
        p.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        final KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(p);
        consumer.subscribe(Arrays.asList("upper-topic3"));

        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));
            for (ConsumerRecord record : records) {
                System.out.println("Received new record: \n" +
                        "Key: " + record.key() + ", " +
                        "Value: " + record.value() + ", " +
                        "Topic: " + record.topic() + ", " +
                        "Partition: " + record.partition() + ", " +
                        "Offset: " + record.offset() + "\n");

            }

        }

    }
    private static String generateRandomString(int length) {
        String characters = "abcdefghijklmnopqrstuvwxyz";
        StringBuilder randomString = new StringBuilder();
        Random random = new Random();
        for (int i = 0; i < length; i++) {
            char randomChar = characters.charAt(random.nextInt(characters.length()));
            randomString.append(randomChar);
        }
        return randomString.toString();
    }
}
