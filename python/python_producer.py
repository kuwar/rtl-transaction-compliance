import json

from confluent_kafka import Producer

from config import kafka_config
from datagen.transaction import generate_transaction

# Initialize Kafka Producer
producer = Producer(kafka_config)

# Define Avro Schema (as a string) or load from schema path
schema_path = "schemas/transaction.avsc"
# avro_schema = fastavro.schema.load_schema(open(schema_path).read())
with open(schema_path, "r") as schema_file:
    avro_schema = schema_file.read()


def delivery_callback(err, msg):
    if err:
        print("ERROR: Message failed delivery: {}".format(err))
    else:
        print(
            "Produced event to topic {topic}: key = {key:12} value = {value:12}".format(
                topic=msg.topic(),
                key=msg.key().decode("utf-8"),
                value=msg.value().decode("utf-8"),
            )
        )


def produce_transaction(
    no_of_transaction: int = 1, topic: str = "transactions"
) -> None:
    for i in range(no_of_transaction):
        transaction_data = generate_transaction()

        producer.produce(
            topic=topic,
            value=json.dumps(transaction_data),
            key=transaction_data["user_id"],
            callback=delivery_callback,
        )

    producer.poll(10000)
    producer.flush()


if __name__ == "__main__":
    produce_transaction(no_of_transaction=1)
