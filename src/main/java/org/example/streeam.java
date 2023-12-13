package org.example;

import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.common.serialization.Serdes;

import java.util.Properties;

public class streeam {
    public static void main(String[] args) {
        Properties config = new Properties();
        config.put(StreamsConfig.APPLICATION_ID_CONFIG, "kafka-streams-app");
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092"); // Replace with your Kafka broker's address.
        config.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        config.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());

        StreamsBuilder builder = new StreamsBuilder();

        KStream<String, String> sourceStream = builder.stream("topic1"); // Replace with your topic name.

        sourceStream.foreach((key, value) -> {
            System.out.println("Received message - Key: " + key + ", Value: " + value);
        });

        Topology topology = builder.build();
        KafkaStreams streams = new KafkaStreams(topology, config);

        streams.start();

        // Add shutdown hook to close the Kafka Streams application gracefully.
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }
}

