from kafka import KafkaProducer
from configs import kafka_config
import json
import uuid
import time
import random

# Створення Kafka Producer
producer = KafkaProducer(
    bootstrap_servers=kafka_config["bootstrap_servers"],
    security_protocol=kafka_config["security_protocol"],
    sasl_mechanism=kafka_config["sasl_mechanism"],
    sasl_plain_username=kafka_config["username"],
    sasl_plain_password=kafka_config["password"],
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    key_serializer=lambda v: json.dumps(v).encode("utf-8"),
)

# Назва топіку
my_name = "danylo"
topic_name = f"{my_name}_building_sensors"


for i in range(1, 500):
    # Відправлення повідомлення в топік
    try:
        data = {
            "sensor_id": random.randint(1, 3),
            "timestamp": time.time(),  # Часова мітка
            "temperature": 0,  # random.randint(25, 45),
            "humidity": 0,  # random.randint(15, 85),
        }
        producer.send(topic_name, key=str(uuid.uuid4()), value=data)
        producer.flush()  # Очікування, поки всі повідомлення будуть відправлені
        print(f"Message {i} sent to topic '{topic_name}' successfully.")
        time.sleep(3)
    except Exception as e:
        print(f"An error occurred: {e}")

producer.close()  # Закриття producer
