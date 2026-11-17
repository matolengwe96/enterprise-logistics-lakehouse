import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp, from_unixtime

def run_silver_weather_transform(spark: SparkSession):
    """reads raw weather/marine JSON payloads from the Unity Catalog volume, parses the JSON structure, and writes to a Silver Delta table."""
    catalog = "logistics_prod"
    raw_schema = "raw_landing"
    volume = "weather_marine"
    silver_schema = "silver_cleaned"
    output_table = f"{catalog}.{silver_schema}.silver_weather_marine"
    
    volume_path = f"/Volumes/{catalog}/{raw_schema}/{volume}"
    print(f"Reading raw weather/marine telemetry from volume: {volume_path}")
    
    # Read raw JSON files from the volume
    raw_df = spark.read.option("multiline", "true").json(volume_path)
    
    # Flatten and parse the nested Open-Meteo JSON structure
    # The JSON structure contains fetch_timestamp, location, and api_response
    cleaned_df = raw_df.select(
        from_unixtime(col("fetch_timestamp")).cast("timestamp").alias("telemetry_timestamp"),
        col("location.region").alias("region"),
        col("location.latitude").alias("latitude"),
        col("location.longitude").alias("longitude"),
        col("api_response.current.temperature_2m").cast("double").alias("temperature_celsius"),
        col("api_response.current.relative_humidity_2m").cast("double").alias("relative_humidity"),
        col("api_response.current.wind_speed_10m").cast("double").alias("wind_speed_10m"),
        col("api_response.current.wind_direction_10m").cast("double").alias("wind_direction_10m"),
        col("status").alias("ingestion_status")
    ).withColumn("processed_at", current_timestamp())
    
    print(f"Writing cleaned weather data to Delta table: {output_table}")
    cleaned_df.write \
        .format("delta") \
        .mode("append") \
        .saveAsTable(output_table)
        
    print("Silver weather transformation completed successfully!")

def main():
    spark = SparkSession.builder \
        .appName("SilverWeatherTransformation") \
        .getOrCreate()
        
    run_silver_weather_transform(spark)

if __name__ == "__main__":
    main()