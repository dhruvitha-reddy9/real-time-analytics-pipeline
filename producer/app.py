import json
import time
import random
from google.cloud import pubsub_v1

PROJECT_ID = "project-7c8b1e72-b1d9-4794-8e1"
TOPIC_ID = "user-events"

publisher = pubsub_v1.PublisherClient()
topic_path = publisher.topic_path(PROJECT_ID, TOPIC_ID)

event_types = ["click", "view", "purchase"]

def generate_event():
    return {
        "user_id": f"U{random.randint(1,100)}",
        "event_type": random.choice(event_types),
        "product_id": f"P{random.randint(1,50)}",
        "timestamp": time.time()
    }

def publish_event():
    event = generate_event()
    publisher.publish(topic_path, json.dumps(event).encode("utf-8"))
    print("Sent:", event)

if __name__ == "__main__":
    while True:
        publish_event()
        time.sleep(1)