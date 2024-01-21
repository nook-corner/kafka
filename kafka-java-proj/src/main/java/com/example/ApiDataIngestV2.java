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

public class ApiDataIngestV2 {

    private static final String INPUT_TOPIC = "aqi"; // Replace with the Kafka topic produced by ApiDataIngest

    public static void main(String[] args) {
        Properties properties = new Properties();
        properties.put(StreamsConfig.APPLICATION_ID_CONFIG, "api-data-streams");
        properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        properties.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        properties.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());

        StreamsBuilder builder = new StreamsBuilder();

        // Consume data from the input topic
        KStream<String, String> inputTopic = builder.stream(INPUT_TOPIC);

        // Process the data (you can customize this part based on your requirements)
        inputTopic
            .filter((key, value) -> {
                try {
                    // Parse JSON response using Jackson
                    ObjectMapper objectMapper = new ObjectMapper();
                    objectMapper.configure(JsonParser.Feature.INCLUDE_SOURCE_IN_LOCATION, true); // Enable INCLUDE_SOURCE_IN_LOCATION
                    JsonNode jsonData = objectMapper.readTree(value);

                    // Filter only records where the city is 'Chatuchak'
                    return jsonData.get("City").asText().equals("Chatuchak");
                } catch (JsonProcessingException e) {
                    // Handle JSON processing exception
                    System.err.println("Error processing JSON: " + e.getMessage());
                    return false;
                }
            })
            .mapValues(value -> {
                try {
                    // Continue processing for 'Chatuchak' records

                    // Extract relevant information
                    ObjectMapper objectMapper = new ObjectMapper();
                    JsonNode jsonData = objectMapper.readTree(value);
                    String city = jsonData.get("City").asText();
                    int aqi = jsonData.get("AQI (US)").asInt();
                    double temperature = jsonData.get("Temperature (°C)").asDouble();
                    int humidity = jsonData.get("Humidity (%)").asInt();

                    // Process the data as needed (print to console for this example)
                    System.out.println("City: " + city + ", AQI: " + aqi +
                            ", Temperature: " + temperature + ", Humidity: " + humidity);

                    // Check AQI value and print warning if it's greater than 100
                    if (aqi > 100) {
                        System.out.println("Warning: High AQI detected!");
                    }

                    // Return a processed value (not used in this example)
                    return "Processed Data";
                } catch (JsonProcessingException e) {
                    // Handle JSON processing exception
                    System.err.println("Error processing JSON: " + e.getMessage());
                    return "Error Processing Data";
                }
            })
            .to("processed_data_topic"); // Replace with your desired output topic

        KafkaStreams streams = new KafkaStreams(builder.build(), properties);
        streams.start();

        // Shutdown hook to correctly close the streams application
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }
}
