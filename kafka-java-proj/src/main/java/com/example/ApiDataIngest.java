package com.example;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.Properties;

public class ApiDataIngest {

    private static final String TOPIC = "aqi"; 
    private static final String BOOTSTRAP_SERVERS = "localhost:9092"; 

    public static void main(String[] args) {

        String apiKey = "";
        String[] cities = {"Chatuchak", "Don%20Mueang", "Lat%20Phrao", "Huai%20Khwang"};

        try {
            while (true) {
                for (String city : cities) {
                    try {
                        String country = "Thailand";
                        String state = "Bangkok";
                        String apiUrl = "http://api.airvisual.com/v2/city?city=" + city + "&state=" + state + "&country=" + country + "&key=" + apiKey;

                        URL url = new URL(apiUrl);
                        HttpURLConnection connection = (HttpURLConnection) url.openConnection();
                        connection.setRequestMethod("GET");
                        int responseCode = connection.getResponseCode();

                        if (responseCode == HttpURLConnection.HTTP_OK) {
                            BufferedReader reader = new BufferedReader(new InputStreamReader(connection.getInputStream()));
                            StringBuilder response = new StringBuilder();
                            String line;

                            while ((line = reader.readLine()) != null) {
                                response.append(line);
                            }

                            reader.close();

                            ObjectMapper objectMapper = new ObjectMapper();
                            JsonNode jsonData = objectMapper.readTree(response.toString());
                            JsonNode pollution = jsonData.get("data").get("current").get("pollution");
                            JsonNode weather = jsonData.get("data").get("current").get("weather");

                            Properties properties = new Properties();
                            properties.put("bootstrap.servers", BOOTSTRAP_SERVERS);
                            properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
                            properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

                            try (Producer<String, String> producer = new KafkaProducer<>(properties)) {
                                String kafkaMessage = String.format("{\"City\":\"%s\",\"State\":\"%s\",\"Country\":\"%s\",\"AQI (US)\":%d,\"Temperature (°C)\":%.1f,\"Humidity (%%)\":%d}",
                                        jsonData.get("data").get("city").asText(),
                                        jsonData.get("data").get("state").asText(),
                                        jsonData.get("data").get("country").asText(),
                                        pollution.get("aqius").asInt(),
                                        weather.get("tp").asDouble(),
                                        weather.get("hu").asInt());

                                producer.send(new ProducerRecord<>(TOPIC, kafkaMessage));
                            }

                        } else {
                            System.out.println("Error: " + responseCode + " " + connection.getResponseMessage());
                        }

                        connection.disconnect();


                        Thread.sleep(5000); //Delay becasue API has limit
                    } catch (IOException | InterruptedException e) {
                        e.printStackTrace();
                    }
                }

                Thread.sleep(60000); // Collect data q 60 sec
            }
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}
