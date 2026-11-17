import os
import json
import requests
from datetime import datetime

def fetch_opensky_data():
    """Fetches global flight state vectors from the OpenSky Network REST API."""
    url = "https://opensky-network.org/api/states/all"
    print(f"[{datetime.utcnow().isoformat()}] Connecting to OpenSky Network API...")
    
    try:
        response = requests.get(url, timeout=30)
        response.raise_for_status()
        data = response.json()
        print(f"Successfully fetched telemetry data. State count: {len(data.get('states', []))}")
        return data
    except requests.exceptions.RequestException as e:
        print(f"Error fetching data from OpenSky API: {e}")
        raise

def land_to_volume(data):
    """Lands raw JSON payload into the Databricks Unity Catalog Managed Volume."""
    # Direct Unity Catalog volume path (pre-provisioned, no makedirs required)
    volume_path = "/Volumes/logistics_prod/raw_landing/opensky_telemetry"
    
    timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    file_path = os.path.join(volume_path, f"opensky_states_{timestamp}.json")
    
    print(f"Writing raw payload to Unity Catalog volume: {file_path}")
    with open(file_path, "w") as f:
        json.dump(data, f)
        
    print("Ingestion complete: file saved successfully.")

if __name__ == "__main__":
    flight_data = fetch_opensky_data()
    if flight_data:
        land_to_volume(flight_data)