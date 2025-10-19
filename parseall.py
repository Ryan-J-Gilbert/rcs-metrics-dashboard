import pandas as pd
from datetime import datetime, date
import os

# -------- USER SETTINGS --------
data_dir = '/project/scv/dugan/sge/data'
queue_info_file = '/projectnb/scv/utilization/katia/queue_info.csv'
output_csv = 'queue_daily_usage.csv'
start_ym = '2015-01'  # <-- starting year-month
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

# Load the queue info
queue_info = pd.read_csv(queue_info_file)[['queuename', 'queuetotal', 'class_util']]

months = month_range(start_ym, end_ym)
files = [os.path.join(data_dir, f"{m}.q") for m in months]

all_results = []

for f in files:
    if not os.path.exists(f):
        print(f"File not found: {f}")
        continue
    print(f"Processing {f}...")

    # Adjust names here as per your new schema!
    df = pd.read_csv(
        f,
        sep=r"\s+",
        header=None,
        na_values=['-NA-'],
        names=['time','queue','ignore_util','cores_util', '??','???','cores_total','?????','??????'],
        on_bad_lines='warn'
    )
    # Parse time
    df['time'] = pd.to_datetime(df['time'], errors='coerce', unit='s')
    n_bad = df['time'].isna().sum()
    if n_bad > 0:
        print(f"Dropping {n_bad} rows where 'time' is unparseable")
    df = df.dropna(subset=['time'])
    df['date'] = df['time'].dt.date

    target_date = date(1970, 1, 1)
    matching_rows = df['time'].dt.date == target_date
    if matching_rows.sum() > 0:
        print(f"Dropping {matching_rows.sum()} rows where date is 1970-01-01")
        df = df[df['time'].dt.date != target_date]

    # Merge queue metadata
    df_meta = pd.merge(
        df,
        queue_info,
        left_on="queue",
        right_on="queuename",
        how='left'
    )[['time', 'date', 'queue', 'cores_util', 'cores_total', 'queuetotal', 'class_util']]

    # --- Daily Aggregation by queuetotal ---
    # Step 1: Sum cores_util for each (date, queuetotal)
    cores_util_day = df_meta.groupby(['date', 'queuetotal'], sort=False)['cores_util'].mean().reset_index()
    print('hm this doesnt quite line up with rcs metrics dash')
    # Step 2: Get header row's cores_total (for the main queue for the group)
    header_daily = (
        df_meta[df_meta['queue'] == df_meta['queuetotal']]
        .drop_duplicates(subset=['date', 'queuetotal'])
        [['date', 'queuetotal', 'cores_total', 'class_util']]
    )
    # Step 3: Merge and calculate util
    out_daily = pd.merge(cores_util_day, header_daily, on=['date', 'queuetotal'], how='left')
    out_daily['util'] = out_daily['cores_util'] / out_daily['cores_total']
    # Optional: tidy output
    result_daily = out_daily[['date', 'queuetotal', 'cores_util', 'cores_total', 'util', 'class_util']]
    # Add file/month info if desired:
    # result_daily['source_file'] = f
    all_results.append(result_daily)

# Combine all months
if all_results:
    final = pd.concat(all_results, ignore_index=True)
    final.to_csv(output_csv, index=False)
    print(f"Combined daily utilization written to: {output_csv}")
else:
    print("No data found in the specified range.")