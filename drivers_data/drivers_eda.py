
#Importerar requests och pandas som dataframe

import requests
import pandas as pd

all_data = []

# Hämtar data för 2023-2025

for year in [2023, 2024, 2025]:
    # Alla sessioner för år
    sessions_url = f"https://api.openf1.org/v1/sessions?year={year}"
    sessions = requests.get(sessions_url).json()
    sessions_df = pd.DataFrame(sessions)

    # Filtrerar fram race-sessioner
    race_sessions = sessions_df[sessions_df["session_name"] == "Race"]

    # Kontrollerar att det finns minst en race-session
    if race_sessions.empty:
        print(f"Inga race-sessioner hittades för {year}")
        continue

    # Tar första race-sessionen för året
    first_race = race_sessions.iloc[0]
    session_key = first_race["session_key"]
    session_name = first_race["session_name"]
    session_type = first_race["session_type"]

    # Hämta driver-data för den sessionen
    drivers_url = f"https://api.openf1.org/v1/drivers?session_key={session_key}"
    drivers = requests.get(drivers_url).json()
    drivers_df = pd.DataFrame(drivers)

    # Kontrollerar att rätt kolumner finns
    needed_driver_cols = [
        "full_name",
        "name_acronym",
        "driver_number",
        "country_code",
        "team_name",
        "session_key"
    ]

    missing_cols = [col for col in needed_driver_cols if col not in drivers_df.columns]
    if missing_cols:
        print(f"Saknade kolumner för {year}: {missing_cols}")
        continue

    drivers_df = drivers_df[needed_driver_cols]

    # Lägger till extra kolumner
    drivers_df["year"] = year
    drivers_df["session_name"] = session_name
    drivers_df["session_type"] = session_type

    # Årets data till listan
    all_data.append(drivers_df)

# Slår ihop all driver data
df = pd.concat(all_data, ignore_index=True)

# Byter namn på kolumner
df = df.rename(columns={
    "full_name": "name",
    "driver_number": "number",
    "country_code": "country",
    "team_name": "team"
})

# Tar bort dubletter
df = df.drop_duplicates()

# Hämtar alla racing sessioner från 2025
sessions_2025 = requests.get("https://api.openf1.org/v1/sessions?year=2025").json()
sessions_2025_df = pd.DataFrame(sessions_2025)

race_sessions_2025 = sessions_2025_df[sessions_2025_df["session_name"] == "Race"]

if race_sessions_2025.empty:
    print("Inga race-sessioner hittades för 2025")
    df.to_csv("drivers_final.csv", index=False)
    print(df.head())
    raise SystemExit


# Hämtar poängställningen från 2025
latest_session = race_sessions_2025["session_key"].max()

championship_url = f"https://api.openf1.org/v1/championship_drivers?session_key={latest_session}"
championship_data = requests.get(championship_url).json()

champ_df = pd.DataFrame(championship_data)

if not champ_df.empty:
    needed_champ_cols = ["driver_number", "points_current", "position_current"]
    champ_df = champ_df[needed_champ_cols]

    champ_df = champ_df.rename(columns={
        "points_current": "current_points_2025",
        "position_current": "championship_position_2025"
    })
else:
    champ_df = pd.DataFrame(columns=[
        "driver_number",
        "current_points_2025",
        "championship_position_2025"
    ])


# Hämtar racing resultaten och plusar ihop wins och podiums
results_list = []

for key in race_sessions_2025["session_key"]:
    try:
        result_url = f"https://api.openf1.org/v1/session_result?session_key={key}"
        result = requests.get(result_url).json()

        if isinstance(result, list) and len(result) > 0:
            results_list.append(pd.DataFrame(result))
        elif isinstance(result, dict):
            results_list.append(pd.DataFrame([result]))
        else:
            print(f"Ingen användbar session_result-data för session {key}")

    except Exception as e:
        print(f"Fel vid hämtning av session {key}: {e}")

# Räknar statistik
if len(results_list) > 0:
    results_df = pd.concat(results_list, ignore_index=True)

    # Dubbelkollar att kolumnerna finns
    if "driver_number" in results_df.columns and "position" in results_df.columns:
        wins_df = (
            results_df[results_df["position"] == 1]
            .groupby("driver_number")
            .size()
            .reset_index(name="wins")
        )

        podiums_df = (
            results_df[results_df["position"] <= 3]
            .groupby("driver_number")
            .size()
            .reset_index(name="podiums")
        )

        stats_df = wins_df.merge(podiums_df, on="driver_number", how="outer").fillna(0)
        stats_df["wins"] = stats_df["wins"].astype(int)
        stats_df["podiums"] = stats_df["podiums"].astype(int)
    else:
        stats_df = pd.DataFrame(columns=["driver_number", "wins", "podiums"])
else:
    stats_df = pd.DataFrame(columns=["driver_number", "wins", "podiums"])


# Mergea championship-datan och racing statistiken
df = df.merge(champ_df, left_on="number", right_on="driver_number", how="left")
df = df.merge(stats_df, left_on="number", right_on="driver_number", how="left")

# Fyller in de tomma värdena
if "wins" in df.columns:
    df["wins"] = df["wins"].fillna(0).astype(int)
else:
    df["wins"] = 0

if "podiums" in df.columns:
    df["podiums"] = df["podiums"].fillna(0).astype(int)
else:
    df["podiums"] = 0

# Tar bort extra kolumner från merget
drop_cols = [col for col in ["driver_number_x", "driver_number_y", "driver_number"] if col in df.columns]
df = df.drop(columns=drop_cols)


# Sparar till en csv fil
df.to_csv("drivers_final.csv", index=False)
