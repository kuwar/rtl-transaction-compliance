import json

from confluent_kafka import Consumer
from config import kafka_config


# Initialize Kafka Consumer
def set_consumer_kafka_conf():
    kafka_config["group.id"] = "transaction_consumer"
    kafka_config["auto.offset.reset"] = "earliest"


set_consumer_kafka_conf()
consumer = Consumer(kafka_config)

# Subscribe to a topic
topic = "transactions"
consumer.subscribe([topic])


# Function to process messages
def process_message(message):
    try:
        print("process_message -> message={}".format(message))
        print("====>>>>" * 20)
        print(f"{json.loads(message.value())}")
        print("====>>>>" * 20)
        print("message.topic={}".format(message.topic()))
        print("message.timestamp={}".format(message.timestamp()))
        print("message.key={}".format(message.key()))
        print("message.value={}".format(message.value()))
        print("message.partition={}".format(message.partition()))
        print("message.offset={}".format(message.offset()))
    except Exception as e:
        print(f"Error deserializing message: {e}")


def consume_transaction():
    # Consume messages
    try:
        print(f"Consuming messages from topic: {topic}")
        while True:
            msg = consumer.poll(1.0)  # Poll for messages with a timeout of 1 second
            if msg is None:
                continue
            if msg.error():
                print(f"Consumer error: {msg.error()}")
                continue

            process_message(msg)

    except KeyboardInterrupt:
        print("Consumer interrupted. Exiting...")
    finally:
        # Close the consumer to commit final offsets
        consumer.close()


if __name__ == "__main__":
    consume_transaction()
