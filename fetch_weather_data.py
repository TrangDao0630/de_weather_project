import requests
import pandas as pd
import sqlite3
import os
import datetime
import time 

# --- Configuration ---
# List of representative locations (Latitude, Longitude, Timezone, Continent, City)
# You can customize this list to include cities relevant to your analysis.
LOCATIONS = [
    # North America
    {"latitude": 34.0522, "longitude": -118.2437, "timezone": "America/Los_Angeles", "continent": "North America", "city": "Los Angeles"},
    {"latitude": 40.7128, "longitude": -74.0060, "timezone": "America/New_York", "continent": "North America", "city": "New York"},
    {"latitude": 45.4215, "longitude": -75.6972, "timezone": "America/Toronto", "continent": "North America", "city": "Ottawa"},

    # South America
    {"latitude": -23.5505, "longitude": -46.6333, "timezone": "America/Sao_Paulo", "continent": "South America", "city": "Sao Paulo"},
    {"latitude": -33.4489, "longitude": -70.6693, "timezone": "America/Santiago", "continent": "South America", "city": "Santiago"},

    # Europe
    {"latitude": 51.5074, "longitude": -0.1278, "timezone": "Europe/London", "continent": "Europe", "city": "London"},
    {"latitude": 48.8566, "longitude": 2.3522, "timezone": "Europe/Paris", "continent": "Europe", "city": "Paris"},
    {"latitude": 52.5200, "longitude": 13.4050, "timezone": "Europe/Berlin", "continent": "Europe", "city": "Berlin"},

    # Asia
    {"latitude": 35.6895, "longitude": 139.6917, "timezone": "Asia/Tokyo", "continent": "Asia", "city": "Tokyo"},
    {"latitude": 28.7041, "longitude": 77.1025, "timezone": "Asia/Kolkata", "continent": "Asia", "city": "New Delhi"},
    {"latitude": 39.9042, "longitude": 116.4074, "timezone": "Asia/Shanghai", "continent": "Asia", "city": "Beijing"},

    # Africa
    {"latitude": -26.2041, "longitude": 28.0473, "timezone": "Africa/Johannesburg", "continent": "Africa", "city": "Johannesburg"},
    {"latitude": 30.0444, "longitude": 31.2357, "timezone": "Africa/Cairo", "continent": "Africa", "city": "Cairo"},

    # Australia/Oceania
    {"latitude": -33.8688, "longitude": 151.2093, "timezone": "Australia/Sydney", "continent": "Australia", "city": "Sydney"},
    {"latitude": -36.8485, "longitude": 174.7633, "timezone": "Pacific/Auckland", "continent": "Australia", "city": "Auckland"}
]

API_BASE_URL = "https://api.open-meteo.com/v1/forecast"
DB_FILE = "weather.db" 

def fetch_current_weather(latitude, longitude, timezone):
    """
    Fetches current weather data from Open-Meteo API.
    Returns a dictionary of weather data or None if an error occurs.
    """
    params = {
        "latitude": latitude,
        "longitude": longitude,
        "current_weather": True,
        "timezone": timezone
    }
    
    try:
        response = requests.get(API_BASE_URL, params=params, timeout=15)
        response.raise_for_status()
        data = response.json()
        
        if 'current_weather' in data:
            return data['current_weather']
        else:
            print(f"Warning: 'current_weather' key not found in API response for {latitude}, {longitude}.")
            return None
            
    except requests.exceptions.HTTPError as e:
        print(f"API HTTP Error for {latitude}, {longitude}: {e.response.status_code} - {e.response.text}")
        return None
    except requests.exceptions.RequestException as e:
        print(f"API request general error for {latitude}, {longitude}: {e}")
        return None

def create_weather_table(conn):
    """
    Creates the weather_records table in the SQLite database if it doesn't exist,
    with a composite primary key for multiple locations.
    """
    create_table_sql = """
    CREATE TABLE IF NOT EXISTS weather_records (
        timestamp TEXT NOT NULL,
        temperature_celsius REAL,
        windspeed_ms REAL,
        weather_code INTEGER,
        latitude REAL NOT NULL,
        longitude REAL NOT NULL,
        timezone TEXT,
        continent TEXT,
        city TEXT,      
        ingestion_timestamp TEXT,
        PRIMARY KEY (timestamp, latitude, longitude)
    );
    """
    try:
        cursor = conn.cursor()
        cursor.execute(create_table_sql)
        conn.commit()
        print(f"SQLite table 'weather_records' processed successfully (created or already existed).")
    except sqlite3.Error as e:
        print(f"Error creating table: {e}")

def store_weather_data(data_frame, conn):
    """
    Stores weather data from a Pandas DataFrame into the SQLite database.
    """
    if data_frame.empty:
        return

    try:
        if 'timestamp' in data_frame.columns and pd.api.types.is_datetime64_any_dtype(data_frame['timestamp']):
            data_frame['timestamp'] = data_frame['timestamp'].dt.strftime('%Y-%m-%dT%H:%M:%S')
        
        if 'ingestion_timestamp' in data_frame.columns and pd.api.types.is_datetime64_any_dtype(data_frame['ingestion_timestamp']):
            data_frame['ingestion_timestamp'] = data_frame['ingestion_timestamp'].dt.strftime('%Y-%m-%dT%H:%M:%S.%f')

        data_frame.to_sql('weather_records', conn, if_exists='append', index=False)
    except sqlite3.IntegrityError:
        pass # Suppress integrity error for duplicate primary keys
    except sqlite3.Error as e:
        print(f"Error storing data to DB: {e}")
    except Exception as e:
        print(f"An unexpected error occurred during data storage: {e}. Data: {data_frame.to_dict('records')}")


def main():
    print(f"--- Starting weather data pipeline for multiple locations ---")

    conn = None
    try:
        conn = sqlite3.connect(DB_FILE)
        create_weather_table(conn)

        records_processed = 0
        for loc in LOCATIONS:
            current_weather_data = fetch_current_weather(loc['latitude'], loc['longitude'], loc['timezone'])
            
            time.sleep(0.1) # Small delay to avoid hitting API rate limits

            if current_weather_data:
                current_weather_data['ingestion_timestamp'] = datetime.datetime.now(datetime.timezone.utc) 
                current_weather_data['latitude'] = loc['latitude']
                current_weather_data['longitude'] = loc['longitude']
                current_weather_data['timezone'] = loc['timezone']
                current_weather_data['continent'] = loc['continent']
                current_weather_data['city'] = loc['city']         

                df = pd.DataFrame([current_weather_data])

                df['timestamp'] = pd.to_datetime(df['time'], errors='coerce') 
                df['temperature_celsius'] = pd.to_numeric(df['temperature'], errors='coerce')
                df['windspeed_ms'] = pd.to_numeric(df['windspeed'], errors='coerce')
                df['weather_code'] = pd.to_numeric(df['weathercode'], errors='coerce').astype('Int64')

                df_to_store = df[[
                    'timestamp', 'temperature_celsius', 'windspeed_ms',
                    'weather_code', 'latitude', 'longitude', 'timezone',
                    'continent', 'city', 'ingestion_timestamp'
                ]].copy() # Explicitly make a copy here to prevent SettingWithCopyWarning
                
                store_weather_data(df_to_store, conn)
                records_processed += 1
            else:
                print(f"Skipping data storage for {loc['city']} due to fetch error.")

        print(f"\n--- Data pipeline finished. Total records processed this run: {records_processed} ---")

    except Exception as e:
        print(f"An error occurred during the main pipeline execution: {e}")
    finally:
        if conn:
            conn.close()
            print("Database connection closed.")

if __name__ == "__main__":
    main()
