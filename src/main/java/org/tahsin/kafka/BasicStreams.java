package org.tahsin.kafka;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Properties;



public class BasicStreams {
    public static void main(String[] args) throws Exception{
        Properties streamsProps = new Properties();

        try (FileInputStream fis = new FileInputStream("src/main/resources/server.properties")) {
            streamsProps.load(fis);
        }

        streamsProps.put(StreamsConfig.APPLICATION_ID_CONFIG, "basic-streams");

        final String inputTopic = "basic-input-topic";
        final String outputTopic = "basic-output-topic";
        final String orderNumberStart = "New";


        StreamsBuilder builder = new StreamsBuilder();

        KStream<String, String> firstStream = builder.stream(inputTopic, Consumed.with(Serdes.String(), Serdes.String()));

        firstStream.peek((key, value) -> System.out.println("Incoming record - key " +key +" value " + value))
                .filter((key, value) -> value.contains(orderNumberStart))
                .mapValues(value -> value.substring(value.indexOf("-") + 1))
                .filter((key, value) -> Long.parseLong(value) > 1000)
                .peek((key, value) -> System.out.println("Outgoing record - key " +key +" value " + value))
                .to(outputTopic, Produced.with(Serdes.String(), Serdes.String()));

        KafkaStreams kafkaStreams = new KafkaStreams(builder.build(), streamsProps);

        kafkaStreams.start();

    }




}
