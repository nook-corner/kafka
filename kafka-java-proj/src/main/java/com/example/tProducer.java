package com.example;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;


public class tProducer {
    public static void main(String[] args) {
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        Producer<String, String> producer = new KafkaProducer<>(properties);

        String topic = "rand_num";
        String[] names = {"John", "Alex", "Sara"};
        int numMessagesPerName = 100;

        for (int i = 1; i <= numMessagesPerName; i++) {
            for (String name : names) {
                String key = "Number";
                // Creating JSON-formatted value
                String value = "{\"Name\": \"" + name + "\", \"Value\": " + i + "}";

                ProducerRecord<String, String> record = new ProducerRecord<>(topic, key, value);

                producer.send(record, (metadata, exception) -> {
                    if (exception == null) {
                        System.out.println("Message sent successfully! Topic: " + metadata.topic() +
                                ", Partition: " + metadata.partition() +
                                ", Offset: " + metadata.offset());
                    } else {
                        System.err.println("Error sending message: " + exception.getMessage());
                        System.err.println("Failed record - Topic: " + record.topic() +
                                ", Key: " + record.key() +
                                ", Value: " + record.value());
                    }
                });

                try {
                    Thread.sleep(10000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }

        producer.flush();
        producer.close();
    }
}
