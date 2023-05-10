import datetime
import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import sort_array, col, lit, min_by, collect_set, concat, struct

from common.config_manager.load_config import get_config


def main():
    dateint_pt = int(datetime.datetime.now().strftime("%Y%m%d"))
    env = os.getenv('F1_ENV')
    config = get_config(env)

    results_path = config['input']['results_path']
    circuits_path = config['input']['circuits_path']
    races_path = config['input']['races_path']
    drivers_path = config['input']['drivers_path']
    output_path = config['output']['path']
    output_format = config['output']['format']

    spark = SparkSession \
        .builder \
        .master('local') \
        .appName("pit_stops_s3_writer") \
        .config("spark.streaming.stopGracefullyOnShutdown", "true") \
        .config("spark.jars.excludes", "com.google.guava:guava") \
        .getOrCreate()

    results_df = spark.read.option("header", "true").csv(results_path)
    drivers_df = spark.read.option("header", "true").csv(drivers_path) \
        .select("driverId", concat(col("forename"), lit(" "), "surname").alias("driverName")) \

    top_3_of_each_race = results_df \
        .select(col("raceId"), col("driverId"), col("position").cast("int").alias("position"), col("time")) \
        .where("position <= 3") \
        .cache()

    top_3_with_names = top_3_of_each_race.join(drivers_df, ["driverId"], "inner")

    top_3_with_names_transpose = top_3_with_names\
        .groupby("raceId").agg(
            min_by("time", "position").alias("winning_time"),
            sort_array(collect_set(struct("position", "driverName")), True).alias("top_3")
        )\
        .selectExpr(
             "raceId AS race_id",
             "winning_time",
             "top_3[0].driverName AS first_place_driver_name",
             "top_3[1].driverName AS second_place_driver_name",
             "top_3[2].driverName AS third_place_driver_name",
         )

    races_df = spark.read.option("header", "true").csv(races_path)\
        .selectExpr("raceId AS race_id", "circuitId AS circuit_id", "name AS race_name", "date AS race_date")
    circuit_df = spark.read.option("header", "true").csv(circuits_path)\
        .selectExpr("circuitId AS circuit_id", "name AS circuit_name")

    races_circuit_names_df = races_df.join(circuit_df, ["circuit_id"], "inner").drop("circuit_id")
    final_res = top_3_with_names_transpose.join(races_circuit_names_df, ["race_id"], "inner")

    final_res\
        .withColumn("dateint_pt", lit(dateint_pt))\
        .repartition(1)\
        .write\
        .mode("overwrite")\
        .partitionBy("dateint_pt")\
        .option("header", "true")\
        .format(output_format)\
        .save(output_path)


if __name__ == '__main__':
    main()
