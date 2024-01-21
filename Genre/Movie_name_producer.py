from confluent_kafka import Producer
import random
from collections import defaultdict
import time
import json

movie_names = [
    (1, "Godfather", 0.6),
    (2, "Shawshank", 0.5),
    (3, "The matrix", 0.4),
    (4, "Forrest gump", 0.05),
    (5, "Green book", 0.05),
    (6, "Green mile", 0.05),
    (7, "007", 0.05),
    (8, "Up", 0.05),
    (9, "Rango", 0.05),
    (10, "The terminal", 0.05),
    (11, "300", 0.05),
    (12, "Star trek", 0.05),
    (13, "In time", 0.05),
    (14, "Django", 0.05)
]

# Define the chances for different event types
chance_click = 0.8
chance_watch = 0.4
chance_add_to_list = 0.2
chance_thumb_up = 0.1

event_types = [
    ("Click", chance_click),
    ("Watch", chance_watch),
    ("Add to List", chance_add_to_list),
    ("Thumb Up", chance_thumb_up)
]

# Kafka configuration
kafka_config = {
    'bootstrap.servers': 'localhost:9092',  
    'client.id': 'movie-event-producer'
}

# Create a Kafka producer instance
producer = Producer(kafka_config)

# Extract movie names and their probabilities
movie_choices = [movie[1] for movie in movie_names]
probabilities = [movie[2] for movie in movie_names]

# Produce data to a Kafka topic

#topic = 'test_mongo_15092023'
topic = 'testv1'

# Loop N times
for i in range(5000):
    # Choose a random event type based on chances
    event_type = random.choices([event[0] for event in event_types], [event[1] for event in event_types])[0]
    random_movie = random.choices(movie_choices, probabilities)[0]

    # Get the current timestamp
    timestamp = int(time.time())

    # Find the index of the chosen movie
    movie_index = next(index for index, name, _ in movie_names if name == random_movie)

    # Create a dictionary to hold the message data
    message_data = {
        "timestamp": timestamp,
        "event_type": event_type,
        "movie_i": movie_index,
        "movie_name": random_movie
    }

    # Convert the dictionary to a JSON string
    json_message = json.dumps(message_data)

    print(json_message)

    # Produce the JSON message to the Kafka topic
    producer.produce(topic, json_message.encode('utf-8'))
    producer.flush()

    time.sleep(2)

# Close the Kafka producer
# producer.close()

