import duckdb
import time
import requests
import pandas as pd

def fetch_openf1(endpoint, params={}, retries=3):
    url = f"https://api.openf1.org/v1/{endpoint}"
    for attempt in range(retries):
        r = requests.get(url, params=params)
        if r.status_code == 429:
            time.sleep(10 * (attempt + 1))
            continue
        r.raise_for_status()
        return pd.DataFrame(r.json())
    raise Exception(f"Failed after {retries} retries: {endpoint}")

print("Hämtar data...")
sessions_df = fetch_openf1("sessions")

# Filtrera bara 2023-2025
sessions_df = sessions_df[sessions_df["year"].isin([2023, 2024, 2025])]

# Skapa sorteringen för sessionerna redan här, fick massa problem i PowerBI. 
sort_mapping = {
    "Practice 1": 1,
    "Practice 2": 2,
    "Practice 3": 3,
    "Sprint Shootout": 4,
    "Sprint Qualifying": 5,
    "Sprint": 6,
    "Qualifying": 7,
    "Race": 8
}
# Skapar en ny kolumn som mappar namnet mot siffran (och sätter 99 om något är okänt)
sessions_df["session_order"] = sessions_df["session_name"].map(sort_mapping).fillna(99)

print(f"Hittade {len(sessions_df)} sessioner. Sparar till DuckDB...")


con = duckdb.connect("f1_data.duckdb") 

# Skriv ÖVER den gamla (Monza) session-tabellen med den nya (Globala)
con.execute("CREATE OR REPLACE TABLE sessions_data AS SELECT * FROM sessions_df")

con.close()
print("Databasen är uppdaterad")