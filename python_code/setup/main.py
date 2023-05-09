from kafka import KafkaAdminClient, KafkaProducer
import os
from kafka.admin import NewTopic
from common.config_manager.load_config import get_config
from common.ksqldbclient.ksqldb_client import KsqlDbClient
import pandas as pd
import json


def main():
    print("setting up system")
    env = os.getenv('F1_ENV')
    config = get_config(env)

    kafka_server = config['kafka']['server']
    kafka_topics_to_create = config['kafka']['topics_to_create']
    base_url_base_url = config['ksqldb']['base_url']
    drivers_path = config['input']['drivers_path']
    drivers_topic = config['kafka']['drivers_topic']

    kafka_admin_client = KafkaAdminClient(
        bootstrap_servers=kafka_server
    )
    create_topics_if_not_exists(kafka_admin_client, kafka_topics_to_create, drivers_topic)

    producer = KafkaProducer(bootstrap_servers=[kafka_server],
                             value_serializer=lambda x: json.dumps(x).encode('utf-8'),
                             acks='all')
    populate_drivers_table(producer, drivers_path, drivers_topic)

    ksqldb_client = KsqlDbClient(base_url_base_url)
    with open('python_code/setup/config/DDL.sql', 'r') as f:
        print(f"Setting up ksqlDb")
        queries_string = f.read()
        queries = queries_string.split(";")

        for query in queries:
            if query:
                print(f"Running ksqlDb query: {query}")
                query = f"{query};"
                ksqldb_client.execute_statement(query)


def populate_drivers_table(producer, drivers_file_name, kafka_output_topic):
    drivers_df = pd.read_csv(drivers_file_name)
    drivers = drivers_df.to_dict('records')

    print(f"populating drivers table")
    for d in drivers:
        producer.send(kafka_output_topic, d)


def create_topics_if_not_exists(admin_client: KafkaAdminClient, topic_names, drivers_topic):
    existing_topics = admin_client.list_topics()

    if drivers_topic in existing_topics:
        existing_topics = existing_topics.remove(drivers_topic)
        admin_client.delete_topics([drivers_topic])

    topic_names_to_create = [NewTopic(name, 1, 1) for name in topic_names if name not in existing_topics]

    if topic_names_to_create:
        print(f"creating kafka topics {', '.join([t.name for t in topic_names_to_create])}")
        admin_client.create_topics(topic_names_to_create)


if __name__ == '__main__':
    main()

