package org.example;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Named;
import org.apache.kafka.streams.kstream.Produced;

import java.util.Properties;

public class colours {
    public static void main(String[] args) {
        Properties config = new Properties();
        config.put(StreamsConfig.APPLICATION_ID_CONFIG, "colour-count1");
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092"); // Replace with your Kafka broker's address.
        config.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        config.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        StreamsBuilder builder = new StreamsBuilder();

        KStream<String, String> colour_input= builder.stream("fav-colour2"); // Replace with your topic name.
        KStream<String, String> outputStream = colour_input.mapValues(value -> value.split(",")[1]);
        String counts = "counts";
        KTable<String, Long> tr_colour = outputStream
            
                .selectKey((key, value) -> value)

                .groupByKey()
                .count(Named.as("counts"));

        KStream<String, Long> countcolour = tr_colour.toStream();

        //wordCountsStream.to(Serdes.String(), Serdes.Long(), "word-count-topic");
        countcolour.to("colour-count2", Produced.with(Serdes.String(), Serdes.Long()));

        Topology topology = builder.build();
        KafkaStreams streams = new KafkaStreams(topology, config);

        streams.start();
        System.out.println(streams.toString());

        // Add shutdown hook to close the Kafka Streams application gracefully.
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }

}

