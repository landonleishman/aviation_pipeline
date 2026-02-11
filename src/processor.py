from pyspark.sql import SparkSession
from pyspark.sql.functions import col, substring, length
import os

jar_path = os.path.abspath("lib/postgresql.jar")
spark = SparkSession.builder \
    .appName("AviationDataPipeline") \
    .config("spark.jars", jar_path) \
    .config("spark.driver.extraClassPath", jar_path) \
    .getOrCreate()

#loading in reference data
airports_df = spark.read.csv("data/reference/airports_new.csv", header=True, inferSchema=True)
airplanes_df = spark.read.csv("data/reference/aircraft-database.csv", header=True, inferSchema=True)

#simple transformation
airports_df = airports_df.withColumn("state", substring(col("iso_region"), length(col("iso_region")) - 1, 2))

#renaming columns for clarity
airports_df = airports_df.withColumnRenamed("name", "airport_name") \
                           .withColumnRenamed("type", "airport_type") \
                           .withColumnRenamed("iso_country", "country") \
                           .withColumnRenamed("municipality", "city") \
                           .withColumnRenamed("latitude_deg", "latitude") \
                           .withColumnRenamed("longitude_deg", "longitude")

airplanes_df = airplanes_df.withColumnRenamed("'icao24'", "icao24") \
                             .withColumnRenamed("'registration'", "registration") \
                             .withColumnRenamed("'manufacturername'", "manufacturer") \
                             .withColumnRenamed("'model'", "model") \
                             .withColumnRenamed("'categorydescription'", "category") \
                             .withColumnRenamed("'owner'", "owner") \
                             .withColumnRenamed("'typecode'", "typecode")

#trimming down to basic schema and pertinent records
refined_airports_df = airports_df.select("iata_code", "airport_name", "airport_type", "country","state" ,"city", "latitude", "longitude")
target_types = ["large_airport", "medium_airport", "small_airport"]
refined_airports_df = refined_airports_df.filter((col("type").isin(target_types)))
refined_airports_df = refined_airports_df.filter(col("iata_code").isNotNull())
refined_airports_df = refined_airports_df.filter(col("country") == "US")

airplanes_df = airplanes_df.select("icao24", "registration", "manufacturer", "model", "category", "owner", "typecode")

db_url = "jdbc:postgresql://localhost:5432/aviation_pipeline"
refined_airports_df.write.format("jdbc").option("url", db_url).option("dbtable", "airports").option("user", "landon").option("password", "password123").mode("overwrite").save()
