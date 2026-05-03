import pandas as pd

laps = pd.read_csv("data/csv/laps_with_positions.csv")

points_map = {1:25, 2:18, 3:15, 4:12, 5:10,
6:8, 7:6, 8:4, 9:2, 10:1}

#Filter to Race sessions first
races = laps[laps['session_type'] == 'Race'].copy()

#Get final position per driver per race (last lap = finishing position)
race_results = (
    races.sort_values('lap_number')
    .groupby(['year', 'session_key', 'driver_number'])
    .last()[['position']]
    .reset_index()
)

#Map points
race_results['points'] = race_results['position'].map(points_map).fillna(0)

#Championship standings per year
championship = (
    race_results.groupby(['year', 'driver_number'])['points']
    .sum()
    .reset_index()
    .sort_values(['year', 'points'], ascending=[True, False])
)

championship.to_csv("data/csv/championship_standings.csv", index=False)
print(championship)