import duckdb
import time
import requests
import pandas as pd

def fetch_openf1(endpoint, params={}, retries=3):
    url = f"https://api.openf1.org/v1/{endpoint}"
    for attempt in range(retries):
        r= requests.get(url, params=params)
        if r.status_code == 429:
            wait = 10 * (attempt +1)
            print (f"Rate limit, wait {wait}s...")
            time.sleep(wait)
            continue
        r.raise_for_status()
        return pd.DataFrame(r.json())
    raise Exception(f"Failed after {retries} retries: {endpoint}")

sessions_df = fetch_openf1("sessions", {"circuit_short_name": "Monza"})
sessions_df = sessions_df[sessions_df["year"] <= 2025]
print(sessions_df[["session_key", "session_name", "date_start", "year", "circuit_short_name"]])


all_laps = []
all_drivers = []
all_weather = []
all_pit = []


for _, session in sessions_df.iterrows():
    key = session["session_key"] 
    year = session["year"]
    session_name = session["session_name"]
    print(f"Fetching {year} Monza  {session_name} (session_key: {key})...")

    all_laps.append(fetch_openf1("laps", {"session_key": key}))
    time.sleep(1.0)
    all_drivers.append(fetch_openf1("drivers", {"session_key": key}))
    time.sleep(1.0)
    all_weather.append(fetch_openf1("weather", {"session_key": key}))
    time.sleep(1.0)
    all_pit.append(fetch_openf1("pit", {"session_key": key}))
    time.sleep(2.0)

laps_df = pd.concat(all_laps, ignore_index=True)
drivers_df = pd.concat(all_drivers, ignore_index=True)
weather_df = pd.concat(all_weather, ignore_index=True)
pit_df = pd.concat(all_pit, ignore_index=True)

con = duckdb.connect("monza_f1_data.duckdb")

con.execute("CREATE OR REPLACE TABLE session AS SELECT * FROM sessions_df")
con.execute("CREATE OR REPLACE TABLE laps AS SELECT * FROM laps_df")
con.execute("CREATE OR REPLACE TABLE drivers AS SELECT * FROM drivers_df")
con.execute("CREATE OR REPLACE TABLE weather AS SELECT * FROM weather_df")
con.execute("CREATE OR REPLACE TABLE pit AS SELECT * FROM pit_df")

con.close()