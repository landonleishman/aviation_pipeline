from pyspark.sql import SparkSession
from pyspark.sql.functions import col, substring, length
from pyspark.sql import functions as F
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



#simple transformations- mapping iso_region to state
airports_df = airports_df.withColumn("state", substring(col("iso_region"), length(col("iso_region")) - 1, 2))

#renaming columns for clarity
airports_df = airports_df.withColumnRenamed("name", "airport_name") \
                           .withColumnRenamed("type", "airport_type") \
                           .withColumnRenamed("iso_country", "country") \
                           .withColumnRenamed("municipality", "city") \
                           .withColumnRenamed("latitude_deg", "latitude") \
                           .withColumnRenamed("longitude_deg", "longitude")

airplanes_df = airplanes_df.withColumnRenamed("'icao24'", "icao24") \
                             .withColumnRenamed("'timestamp'", "timestamp") \
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


#-------airplanes df trimming-----------
airplanes_df = airplanes_df.select("icao24", "timestamp", "manufacturer", "model", "category", "owner", "typecode")


#-------airplanes df cleaning----------- 
airplanes_df = airplanes_df.withColumn(
    "icao24", F.lpad(F.lower(F.trim(F.col("icao24"))), 6, "0")) #getting rid of whitespace, converting to lowercase, and padding with zeros to ensure 6 characters

airplanes_df = airplanes_df.withColumn(
    "timestamp", 
    F.expr("try_cast(regexp_replace(timestamp, \"['\\\"]\", '') as timestamp)")) # getting rid of quotes and converting to timestamp

col_names = ['manufacturer', 'model', 'category', 'owner', 'typecode']
for col_name in col_names:
    airplanes_df = airplanes_df.withColumn(
        col_name,
        F.when(
            (F.trim(F.col(col_name)) == "") | 
            (F.trim(F.col(col_name)) == '"') | 
            (F.trim(F.col(col_name)) == "''"), 
            None
        ).otherwise(F.col(col_name)))

#-------fixing data types-----------    
airplanes_df = airplanes_df.withColumn("timestamp", F.col("timestamp").cast("timestamp")) #casting timestamp to a timestamp data type



#inserting
db_url = "jdbc:postgresql://localhost:5432/aviation_pipeline"
refined_airports_df.write.format("jdbc").option("url", db_url).option("dbtable", "airports").option("user", "landon").option("password", "password123").mode("overwrite").save()
airplanes_df.write.format("jdbc").option("url", db_url).option("dbtable", "airplanes").option("user", "landon").option("password", "password123").mode("overwrite").save()