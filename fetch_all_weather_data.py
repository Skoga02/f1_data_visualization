import duckdb
import time
import requests
import pandas as pd

def fetch_openf1(endpoint, params={}, retries=3):
    url = f"https://api.openf1.org/v1/{endpoint}"
    for attempt in range(retries):
        r = requests.get(url, params=params)
        if r.status_code == 429:
            wait = 10 * (attempt + 1)
            print(f"Rate limit, wait {wait}s...")
            time.sleep(wait)
            continue
        r.raise_for_status()
        return pd.DataFrame(r.json())
    raise Exception(f"Failed after {retries} retries: {endpoint}")


sessions_df = fetch_openf1("sessions")

# Filtrera så de bara är 2023, 2024 och 2025
sessions_df = sessions_df[sessions_df["year"].isin([2023, 2024, 2025])]
totalt_antal = len(sessions_df)
print(f"Hittade totalt {totalt_antal} sessioner att hämta.\n")


all_weather = []


for i, (_, session) in enumerate(sessions_df.iterrows(), 1):
    key = session["session_key"] 
    year = session["year"]
    session_name = session["session_name"]
    circuit = session["circuit_short_name"]
    
    print(f"[{i}/{totalt_antal}] Fetching Weather for {year} {circuit} - {session_name}...")

    try:
        raw_weather = fetch_openf1("weather", {"session_key": key})
        time.sleep(1.0) 
        
        # Trimmar vädret till 5 minuter direkt
        if not raw_weather.empty:
            raw_weather['date'] = pd.to_datetime(raw_weather['date'], format='ISO8601')
            numeric_cols = ['pressure', 'track_temperature', 'rainfall', 'wind_speed', 'wind_direction', 'humidity', 'air_temperature']
            
            clean_weather = (
                raw_weather.set_index('date')
                [numeric_cols]
                .resample('5min')
                .mean()
                .dropna()
                .reset_index()
            )
            clean_weather = clean_weather.round(1)
            clean_weather['session_key'] = key
            all_weather.append(clean_weather)
            
    except requests.exceptions.HTTPError as e:
        # Om vi får fel, skippa 
        print(f" Ingen väderdata finns för denna session.")
        continue

# Slår ihop den städade väderdatan
weather_df = pd.concat(all_weather, ignore_index=True) if all_weather else pd.DataFrame()

print("Sparar till DuckDB...")
# Bytte namn till f1_weather_only.duckdb så den inte skrivs över av monza_data.py som också har väderdata
con = duckdb.connect("f1_weather_only.duckdb") 

if not sessions_df.empty: con.execute("CREATE OR REPLACE TABLE session AS SELECT * FROM sessions_df")
if not weather_df.empty: con.execute("CREATE OR REPLACE TABLE weather AS SELECT * FROM weather_df")

con.close()
print("Done!")