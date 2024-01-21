package com.example;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.KStream;

import java.util.Properties;

public class ApiDataProcess {

    private static final String INPUT_TOPIC = "aqi";

    public static void main(String[] args) {
        Properties properties = new Properties();
        properties.put(StreamsConfig.APPLICATION_ID_CONFIG, "api-data-streams");
        properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        properties.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        properties.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());

        StreamsBuilder builder = new StreamsBuilder();

        KStream<String, String> inputTopic = builder.stream(INPUT_TOPIC);
  
        inputTopic
            .filter((key, value) -> {
                try {
                    ObjectMapper objectMapper = new ObjectMapper();
                    objectMapper.configure(JsonParser.Feature.INCLUDE_SOURCE_IN_LOCATION, true); 
                    JsonNode jsonData = objectMapper.readTree(value);

                    return jsonData.get("City").asText().equals("Chatuchak"); // Filter Chatuchak
                } catch (JsonProcessingException e) {
                    // Handle JSON processing exception
                    System.err.println("Error processing JSON: " + e.getMessage());
                    return false;
                }
            })
            .mapValues(value -> {
                try {

                    ObjectMapper objectMapper = new ObjectMapper();
                    JsonNode jsonData = objectMapper.readTree(value);
                    String city = jsonData.get("City").asText();
                    int aqi = jsonData.get("AQI (US)").asInt();
                    double temperature = jsonData.get("Temperature (°C)").asDouble();
                    int humidity = jsonData.get("Humidity (%)").asInt();

                    System.out.println("City: " + city + ", AQI: " + aqi +
                            ", Temperature: " + temperature + ", Humidity: " + humidity);

                    // stimulated condition
                    if (aqi > 100) {
                        System.out.println("Warning: High AQI detected!");
                    }
                    return "Processed Data";

                } catch (JsonProcessingException e) {
                    System.err.println("Error processing JSON: " + e.getMessage());
                    return "Error Processing Data";
                }
            })
            .to("processed_data_topic"); // output topic

        KafkaStreams streams = new KafkaStreams(builder.build(), properties);
        streams.start();

        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }
}
