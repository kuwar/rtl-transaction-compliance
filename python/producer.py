from confluent_kafka import Producer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import SerializationContext, MessageField

from config import kafka_config, sr_config
from datagen.transaction import generate_transaction

# Initialize Kafka Producer
producer = Producer(kafka_config)

# Initialize the Schema Registry Client
schema_registry_client = SchemaRegistryClient(sr_config)

# Define Avro Schema (as a string) or load from schema path
schema_path = "schemas/transaction.avsc"
# avro_schema = fastavro.schema.load_schema(open(schema_path).read())
with open(schema_path, "r") as schema_file:
    avro_schema = schema_file.read()

# Create Avro Serializer
avro_serializer = AvroSerializer(schema_registry_client, avro_schema)


def delivery_callback(err, msg):
    if err:
        print("ERROR: Message failed delivery: {}".format(err))
    else:
        print(
            "Produced event to topic {topic}: key = {key} value = {value}".format(
                topic=msg.topic(),
                key=msg.key(),
                value=msg.value(),
            )
        )


def produce_transaction(
    no_of_transaction: int = 1, topic: str = "transactions"
) -> None:
    for i in range(no_of_transaction):
        transaction_data = generate_transaction()

        serialized_transaction_data = avro_serializer(
            transaction_data, SerializationContext(topic, MessageField.VALUE)
        )

        producer.produce(
            topic=topic,
            value=serialized_transaction_data,
            key=transaction_data["user_id"],
            callback=delivery_callback,
        )

    producer.poll(10000)
    producer.flush()


if __name__ == "__main__":
    produce_transaction(no_of_transaction=10)
