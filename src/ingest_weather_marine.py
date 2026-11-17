import os
import json
import urllib.request
from datetime import datetime, timezone
from pyspark.sql import SparkSession

def fetch_weather_marine_data():
    """Fetches real-time weather and marine telemetry from Open-Meteo public API."""
    print("Fetching live weather and marine telemetry data from API...")
    
    # Coordinates for Cape Town coastal area (-33.9249, 18.4241)
    latitude = -33.9249
    longitude = 18.4241
    
    # Open-Meteo Marine & Weather API endpoint
    api_url = (
        f"https://api.open-meteo.com/v1/forecast?"
        f"latitude={latitude}&longitude={longitude}"
        f"&current=temperature_2m,relative_humidity_2m,wind_speed_10m,wind_direction_10m"
        f"&hourly=wave_height,wave_direction,sea_surface_temperature"
    )
    
    try:
        req = urllib.request.Request(
            api_url, 
            headers={"User-Agent": "EnterpriseLogisticsLakehouse/1.0"}
        )
        with urllib.request.urlopen(req) as response:
            if response.status == 200:
                data = json.loads(response.read().decode("utf-8"))
                payload = {
                    "fetch_timestamp": int(datetime.now(timezone.utc).timestamp()),
                    "location": {"latitude": latitude, "longitude": longitude, "region": "Cape Town Coastal"},
                    "api_response": data,
                    "status": "success"
                }
                print("Successfully fetched live telemetry data from API.")
                return payload
            else:
                print(f"API request failed with status code: {response.status}")
    except Exception as e:
        print(f"Error fetching live API data: {e}")
        
    # Fallback payload if network is unavailable
    return {
        "fetch_timestamp": int(datetime.now(timezone.utc).timestamp()),
        "status": "fallback_error"
    }

def main():
    spark = SparkSession.builder.getOrCreate()
    
    # Define Unity Catalog Volume path for weather & marine raw landing
    catalog = "logistics_prod"
    schema = "raw_landing"
    volume = "weather_marine"
    
    volume_path = f"/Volumes/{catalog}/{schema}/{volume}"
    
    # Ensure directory path is handled natively
    if not os.path.exists(volume_path):
        os.makedirs(volume_path, exist_ok=True)
        
    # Fetch live data
    data = fetch_weather_marine_data()
    
    # Generate timestamped filename
    timestamp_str = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    filename = f"weather_marine_states_{timestamp_str}.json"
    full_file_path = os.path.join(volume_path, filename)
    
    # Write raw JSON payload to Unity Catalog Volume
    print(f"Writing raw weather/marine payload to Unity Catalog volume: {full_file_path}")
    with open(full_file_path, "w") as f:
        json.dump(data, f, indent=4)
        
    print("Weather and marine ingestion complete: file saved successfully.")

if __name__ == "__main__":
    main()