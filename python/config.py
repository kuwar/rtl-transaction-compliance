kafka_config = {
    "bootstrap.servers": "192.168.1.17:9092,192.168.1.17:9093,192.168.1.17:9094",
    "security.protocol": "PLAINTEXT",
    # 'security.protocol': 'SASL_SSL',
    # 'sasl.mechanisms': 'PLAIN',
    # 'sasl.username': '<CLUSTER_API_KEY>',
    # 'sasl.password': '<CLUSTER_API_SECRET>'
}

sr_config = {
    "url": "http://192.168.1.17:8081/",
    # 'basic.auth.user.info': '<SR_API_KEY>:<SR_API_SECRET>'
}
