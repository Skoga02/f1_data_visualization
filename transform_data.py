import pandas as pd

df_drivers = pd.read_csv("data/temporary_data/driver_standings.csv")
df_constructors = pd.read_csv("data/temporary_data/constructor_standings.csv")

df_drivers_filtered = df_drivers[df_drivers['season'].isin([2023, 2024, 2025])].copy()
df_constructors_filtered = df_constructors[df_constructors['season'].isin([2023, 2024, 2025])].copy()

df_drivers_filtered.rename(columns={'season': 'year'}, inplace=True)
df_constructors_filtered.rename(columns={'season': 'year'}, inplace=True)

df_drivers_filtered.to_csv('driver_standings_2023_2025.csv', index=False)
df_constructors_filtered.to_csv('constructor_standings_2023_2025.csv', index=False)