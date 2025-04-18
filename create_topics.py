from kafka.admin import KafkaAdminClient, NewTopic
from configs import kafka_config

# Створення клієнта Kafka
admin_client = KafkaAdminClient(
    bootstrap_servers=kafka_config["bootstrap_servers"],
    security_protocol=kafka_config["security_protocol"],
    sasl_mechanism=kafka_config["sasl_mechanism"],
    sasl_plain_username=kafka_config["username"],
    sasl_plain_password=kafka_config["password"],
)

# Визначення нового топіку
my_name = "danylo"
num_partitions = 2
replication_factor = 1

topic_name = f"{my_name}_building_sensors"
building_sensors = NewTopic(
    name=topic_name,
    num_partitions=num_partitions,
    replication_factor=replication_factor,
)

topic_name = f"{my_name}_temperature_alerts"
temperature_alerts = NewTopic(
    name=topic_name,
    num_partitions=num_partitions,
    replication_factor=replication_factor,
)

topic_name = f"{my_name}_humidity_alerts"
humidity_alerts = NewTopic(
    name=topic_name,
    num_partitions=num_partitions,
    replication_factor=replication_factor,
)

# Створення нового топіку
try:
    admin_client.create_topics(
        new_topics=[building_sensors, temperature_alerts, humidity_alerts],
        validate_only=False,
    )
    print(f"Topics created successfully.")
except Exception as e:
    print(f"An error occurred: {e}")

print([topic for topic in admin_client.list_topics() if "danylo" in topic])

# Закриття зв'язку з клієнтом
admin_client.close()
