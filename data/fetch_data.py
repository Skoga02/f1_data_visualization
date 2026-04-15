import duckdb
import requests
import pandas as pd 

con = duckdb.connect("f1_data.duckdb")

def fetch_openf1(endpoint, params={}):
    url = f"https://api.openf1.org/v1/{endpoint}"
    r = requests.get(url, params=params)
    return pd.DataFrame(r.json())

session_result_df = fetch_openf1("session_result", {"session_key": 9839})
laps_df = fetch_openf1("laps", {"session_key": 9839})
drivers_df = fetch_openf1("drivers", {"session_key": 9839})
weather_df = fetch_openf1("weather", {"session_key": 9839})
    
con.execute("CREATE OR REPLACE TABLE session_result AS SELECT * FROM session_result_df")
con.execute("CREATE OR REPLACE TABLE laps AS SELECT * FROM laps_df")
con.execute("CREATE OR REPLACE TABLE drivers AS SELECT * FROM drivers_df")
con.execute("CREATE OR REPLACE TABLE weather AS SELECT * FROM weather_df")

con.close()