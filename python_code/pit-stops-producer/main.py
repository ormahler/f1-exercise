import pandas as pd
from kafka import KafkaProducer
import json

from python_code.common.config_manager.load_config import get_config
from utils.utils import generate_record_id
import os


def main():
    env = os.getenv('F1_ENV')
    config = get_config(env)

    pit_stops_file_name = config['input']['pit_stops_file_name']
    races_file_name = config['input']['races_file_name']
    kafka_server = config['kafka']['server']
    kafka_output_topic = config['kafka']['output_topic']

    pit_stops_df = pd.read_csv(pit_stops_file_name)
    races_df = pd.read_csv(races_file_name)
    races_df = races_df[['raceId', 'date']]
    pit_stops_df = pit_stops_df.rename(columns={'milliseconds': 'duration_milliseconds'})
    pit_stops_df = pit_stops_df.merge(races_df, "left", on=["raceId"])
    pit_stops_df['id'] = pit_stops_df.apply(lambda x: generate_record_id(x.driverId, x.date, x.time), axis=1)
    pit_stops = pit_stops_df.to_dict('records')

    producer = KafkaProducer(bootstrap_servers=[kafka_server],
                             value_serializer=lambda x: json.dumps(x).encode('utf-8'),
                             acks='all')

    for d in pit_stops:
        producer.send(kafka_output_topic, d, str(d['id']).encode())


if __name__ == '__main__':
    main()


