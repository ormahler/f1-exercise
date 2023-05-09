import os

from pyspark.sql import SparkSession

from common.config_manager.load_config import get_config


def main():
    env = os.getenv('F1_ENV')
    config = get_config(env)

    check_point_dir = config['spark']['check_point_dir']
    server = config['kafka']['server']
    input_topic = config['kafka']['input_topic']
    output_topic = config['kafka']['output_topic']
    output_trigger_interval = config['output']['trigger_interval']

    spark = SparkSession \
        .builder \
        .master('local') \
        .appName('pit_stops_s3_writer') \
        .config('spark.jars.packages', 'org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.0') \
        .getOrCreate()

    input_df = spark.readStream \
        .format('kafka') \
        .option('kafka.bootstrap.servers', server) \
        .option('subscribe', input_topic) \
        .option('startingOffsets', 'earliest') \
        .load()

    input_df \
        .withWatermark("timestamp", "3 days") \
        .dropDuplicates(["key"]) \
        .selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)") \
        .writeStream \
        .format("kafka") \
        .outputMode("append") \
        .option("kafka.bootstrap.servers", server) \
        .option("topic", output_topic) \
        .option("checkpointLocation", check_point_dir) \
        .trigger(processingTime=output_trigger_interval) \
        .start() \
        .awaitTermination()


if __name__ == '__main__':
    main()
