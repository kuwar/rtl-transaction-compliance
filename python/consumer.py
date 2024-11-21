from confluent_kafka import Consumer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer
from confluent_kafka.serialization import SerializationContext, MessageField

from config import kafka_config, sr_config


# Initialize Kafka Consumer
def set_consumer_kafka_conf():
    kafka_config["group.id"] = "transaction_consumer"


set_consumer_kafka_conf()
consumer = Consumer(kafka_config)

# Initialize Schema Registry Client
schema_registry_client = SchemaRegistryClient(sr_config)

# Create Avro Deserializer
avro_deserializer = AvroDeserializer(
    schema_registry_client=schema_registry_client,
    schema_str=None,  # Schema is fetched from Schema Registry based on message
)

# Subscribe to a topic
topic = "transactions"
consumer.subscribe([topic])


# Function to process messages
def process_message(message):
    try:
        # Deserialize the message
        deserialized_value = avro_deserializer(
            message.value(), SerializationContext(message.topic(), MessageField.VALUE)
        )
        print("process_message -> message={}".format(message))
        print("====>>>>" * 20)
        print(f"{deserialized_value}")
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
