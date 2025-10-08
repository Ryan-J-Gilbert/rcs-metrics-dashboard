import pandas as pd
from datetime import datetime
import os

# -------- USER SETTINGS --------
data_dir = '/project/scv/dugan/sge/data'
queue_info = '/projectnb/scv/utilization/katia/queue_info.csv'
output_csv = 'queue_daily_usage.csv'
start_ym = '2024-01'  # <-- starting year-month
end_ym   = '2025-01'  # <-- ending year-month
# --------------------------------

# Helper: list year-month strings
def month_range(start_ym, end_ym):
    start = datetime.strptime(start_ym, "%Y-%m")
    end   = datetime.strptime(end_ym, "%Y-%m")
    ym_list = []
    while start <= end:
        ym_list.append(start.strftime('%y%m'))  # YYMM format
        # move to next month
        if start.month == 12:
            start = start.replace(year=start.year+1, month=1)
        else:
            start = start.replace(month=start.month+1)
    return ym_list

# List files to process
months = month_range(start_ym, end_ym)
files = [os.path.join(data_dir, f"{m}.q") for m in months]

df_all = []

for f in files:
    if not os.path.exists(f):
        print(f"File not found: {f}")
        continue
    print(f"Processing {f}...")
    df = pd.read_csv(
        f,
        sep='\s+',
        header=None,
        na_values=['-NA-'],
        names=['time','queue','util?','?', '??','???','????','?????','??????'],
        on_bad_lines='warn'
    )
    
    
    # df['time'] = pd.to_datetime(df['time'])
    # df['date'] = df['time'].dt.date


    # Attempt to parse
    df['time'] = pd.to_datetime(df['time'], errors='coerce', unit='s')
    n_bad = df['time'].isna().sum()
    if n_bad > 0:
        print(f"Dropping {n_bad} rows where 'time' is unparseable")
        # Optional: save bad rows for later review
        # df[df['time'].isna()].to_csv('bad_times_{}.csv'.format(os.path.basename(f)), index=False)
    df = df.dropna(subset=['time'])
    df['date'] = df['time'].dt.date
    # daily mean aggregation per queue
    agg = (
        df
        .groupby(['date', 'queue'], as_index=False)['util?']
        .mean()
        .rename(columns={'util?': 'util_mean'})
    )
    df_all.append(agg)

# Combine all
if df_all:
    result = pd.concat(df_all, ignore_index=True)
    # Get queue info to join
    queue_info = pd.read_csv(queue_info)[['queuename', 'class_util']]
    final = pd.merge(result, queue_info, left_on='queue', right_on='queuename', how='left')
    final.to_csv(output_csv, index=False)
    print(f"Combined daily usage written to: {output_csv}")
else:
    print("No data found in the specified range.")