package org.tahsin.kafka;

import org.apache.kafka.clients.producer.*;

import java.io.FileInputStream;
import java.util.Properties;

public class simpleProducer {
    public static void main(String[] args) throws Exception {
        Properties streamsProps = new Properties();

        try (FileInputStream fis = new FileInputStream("src/main/resources/server.properties")) {
            streamsProps.load(fis);
        }

        streamsProps.put(ProducerConfig.ACKS_CONFIG, "all");
        streamsProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        streamsProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");

        final String topic = "basic-input-topic";

        Producer<String, String> producer = new KafkaProducer<String, String>(streamsProps);

        String key = "5";
        String value = "New-20";

        System.out.printf("Producing record: %s\t%s%n", key, value);
        producer.send(new ProducerRecord<String, String>(topic, key, value), new Callback() {
            @Override
            public void onCompletion(RecordMetadata m, Exception e) {
                if (e != null) {
                    e.printStackTrace();
                } else {
                    System.out.printf("Produced record to topic %s partition [%d] @ offset %d%n", m.topic(), m.partition(), m.offset());
                }
            }
        });

        producer.flush();

        producer.close();
    }
}
