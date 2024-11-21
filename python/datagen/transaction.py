from faker import Faker
import random

fake = Faker()


def generate_transaction():
    return {
        "transaction_id": fake.uuid4(),
        "user_id": fake.uuid4(),
        "amount": round(random.uniform(10.0, 5000.0), 2),
        "timestamp": fake.date_time_this_year().isoformat(),
        "geo_location": {"lat": str(fake.latitude()), "lon": str(fake.longitude())},
        "ip_address": fake.ipv4(),
    }


if __name__ == "__main__":
    transactions = [generate_transaction() for _ in range(1000)]

    print(transactions)
