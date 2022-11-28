package org.tahsin.kafka;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;

import java.io.FileInputStream;
import java.io.InputStream;
import java.util.Arrays;
import java.util.Properties;

public class streamStarterApp {
    public static void main(String[] args) throws Exception {
        // Read the Kafka Server's ip address from the properties file
        Properties kafkaServerIPProperties = new Properties();
        InputStream is = new FileInputStream("server.properties");
        kafkaServerIPProperties.load(is);


        Properties properties = new Properties();

        properties.put(StreamsConfig.APPLICATION_ID_CONFIG, "kafka-stream-word-count");
        properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaServerIPProperties.getProperty("bootstrap.servers"));
        properties.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        properties.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        StreamsBuilder streamsBuilder = new StreamsBuilder();

        // Step-1 :- Stream from kafka
        KStream<String, String> wordCountInput = streamsBuilder.stream("word-count-input");

        // Step-2 :- Lowercase the message value
        wordCountInput.mapValues(value -> value.toLowerCase())

        // Step-3 :- Split the value by space and make it to array
                .flatMapValues(value -> Arrays.asList(value.split(" ")))
        // Step-4 :-
    }
}
