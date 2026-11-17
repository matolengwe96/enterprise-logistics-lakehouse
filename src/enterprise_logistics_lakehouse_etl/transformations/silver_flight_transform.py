import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp, from_unixtime, explode

def run_silver_flight_transform(spark: SparkSession):
    """Reads raw OpenSky JSON telemetry from the Unity Catalog volume, explodes state vectors, and writes to Silver Delta table."""
    catalog = "logistics_prod"
    raw_schema = "raw_landing"
    volume = "opensky_telemetry"
    silver_schema = "silver_cleaned"
    output_table = f"{catalog}.{silver_schema}.silver_flight_telemetry"
    
    volume_path = f"/Volumes/{catalog}/{raw_schema}/{volume}"
    print(f"Reading raw flight telemetry from: {volume_path}")
    
    raw_df = spark.read.option("multiline", "true").json(volume_path)
    
    # Flatten the JSON: select time and explode the 'states' array
    parsed_df = raw_df.select(
        from_unixtime(col("time")).cast("timestamp").alias("telemetry_timestamp"),
        explode(col("states")).alias("state_vector")
    )
    
    cleaned_df = parsed_df.select(
        col("telemetry_timestamp"),
        col("state_vector")[0].cast("string").alias("icao24"),
        col("state_vector")[1].cast("string").alias("callsign"),
        col("state_vector")[2].cast("string").alias("origin_country"),
        col("state_vector")[5].cast("double").alias("longitude"),
        col("state_vector")[6].cast("double").alias("latitude"),
        col("state_vector")[7].cast("double").alias("baro_altitude"),
        col("state_vector")[9].cast("double").alias("velocity"),
    ).withColumn("processed_at", current_timestamp())
    
    # Drop existing table to clear out old schema mismatch, then recreate
    spark.sql(f"DROP TABLE IF EXISTS {output_table}")
    
    print(f"Writing cleaned data to Delta table: {output_table}")
    cleaned_df.write \
        .format("delta") \
        .saveAsTable(output_table)
        
    print("Silver flight transformation completed successfully!")

def main():
    spark = SparkSession.builder \
        .appName("SilverFlightTransformation") \
        .getOrCreate()
        
    run_silver_flight_transform(spark)

if __name__ == "__main__":
    main()