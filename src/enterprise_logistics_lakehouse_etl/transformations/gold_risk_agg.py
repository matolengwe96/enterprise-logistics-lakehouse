from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, col, count, countDistinct, current_timestamp, round as spark_round, sum as spark_sum

def run_gold_risk_agg(spark: SparkSession):
    """Combines Air, Sea, Land, and Weather Silver tables into a unified Gold risk summary."""
    catalog = "logistics_prod"
    silver_schema = "silver_cleaned"
    gold_schema = "gold"

    flight_df = spark.read.table(f"{catalog}.{silver_schema}.silver_flight_telemetry")
    weather_df = spark.read.table(f"{catalog}.{silver_schema}.silver_weather_marine")
    maritime_df = spark.read.table(f"{catalog}.{silver_schema}.silver_maritime_ais")
    traffic_df = spark.read.table(f"{catalog}.{silver_schema}.silver_terrestrial_traffic")

    # 1. Air Telemetry Summary by Origin Country
    flight_summary = flight_df.groupBy("origin_country").agg(
        count("icao24").alias("active_flights_count"),
        spark_round(avg("baro_altitude"), 2).alias("avg_altitude_meters"),
        spark_round(avg("velocity"), 2).alias("avg_velocity_mps")
    )

    # 2. Latest Cape Town Coastal Weather
    latest_weather = weather_df.orderBy(col("telemetry_timestamp").desc()).limit(1).select(
        col("temperature_celsius"),
        col("wind_speed_10m"),
        col("relative_humidity"),
        col("region").alias("weather_monitoring_region")
    )

    # 3. Live AISstream Vessel Summary
    maritime_summary = maritime_df.agg(
        countDistinct("mmsi").alias("unique_vessels_tracked"),
        spark_round(avg("speed_knots"), 2).alias("avg_vessel_speed_knots")
    )

    # 4. Live TomTom Terrestrial Corridor Summary
    traffic_summary = traffic_df.agg(
        spark_round(avg("congestion_index"), 2).alias("avg_road_congestion_index"),
        spark_sum("active_incidents").alias("total_road_incidents")
    )

    # Cross-join environmental, maritime, and road context onto flight operations
    enriched_gold_df = (
        flight_summary
        .crossJoin(latest_weather)
        .crossJoin(maritime_summary)
        .crossJoin(traffic_summary)
        .withColumn("updated_at", current_timestamp())
    )

    output_table = f"{catalog}.{gold_schema}.flight_risk_summary"
    spark.sql(f"DROP TABLE IF EXISTS {output_table}")
    print(f"Writing tri-modal aggregated data to Gold table: {output_table}")
    enriched_gold_df.write.format("delta").saveAsTable(output_table)

    print("Gold layer tri-modal aggregation completed successfully!")

def main():
    spark = SparkSession.builder.appName("GoldTriModalRiskAggregation").getOrCreate()
    run_gold_risk_agg(spark)

if __name__ == "__main__":
    main()