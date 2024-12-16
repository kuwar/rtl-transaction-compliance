kafka_config = {
    "bootstrap.servers": "localhost:9092,localhost:9093,localhost:9094",
    "security.protocol": "PLAINTEXT",
    # 'security.protocol': 'SASL_SSL',
    # 'sasl.mechanisms': 'PLAIN',
    # 'sasl.username': '<CLUSTER_API_KEY>',
    # 'sasl.password': '<CLUSTER_API_SECRET>'
}

sr_config = {
    "url": "http://localhost:8081/",
    # 'basic.auth.user.info': '<SR_API_KEY>:<SR_API_SECRET>'
}

es_config = {
    "clusters": ["http://127.0.0.1:9200"]
}

pg_config = {
    "url": "jdbc:postgresql://localhost:5432/postgres",
    "dbtable": "<TABLE_NAME>",
    "user": "postgres",
    "password": "postgres"
}
