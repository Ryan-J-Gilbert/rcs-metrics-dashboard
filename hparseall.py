import pandas as pd
import re
from datetime import datetime, date
import os

# -------- USER SETTINGS --------
data_dir = '/project/scv/dugan/sge/data'
queue_info_file = '/projectnb/scv/utilization/katia/queue_info.csv'
output_csv = 'hqueue_daily_usage.csv'
start_ym = '2025-01'  # <-- starting year-month
end_ym   = '2025-03'  # <-- ending year-month
# --------------------------------

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

queue_info = pd.read_csv(queue_info_file)[['queuename', 'queuetotal', 'class_util']]

months = month_range(start_ym, end_ym)
files = [os.path.join(data_dir, f"{m}.h") for m in months]
all_results = []

# Patterns for node and queue lines
node_pat = re.compile(r'^(\d+)\s+(\S+)\s+\S+\s+(\d+)')
queue_pat = re.compile(r'^(\d+)\s+ +(\S+)\s+\S+\s+(\d+)/(\d+)/(\d+)')

for f in files:
    if not os.path.exists(f):
        print(f"File not found: {f}")
        continue
    print(f"Processing {f}...")
    
    # Parse manually, build rows: timestamp, node, ncpu, queue, used_cores, total_cores
    parsed_rows = []
    with open(f) as fin:
        curr_timestamp = None
        curr_hostname = None
        curr_ncpu = None
        for line in fin:
            line = line.strip()
            node_match = node_pat.match(line)
            queue_match = queue_pat.match(line)
            if node_match and not queue_match:
                curr_timestamp = int(node_match.group(1))
                curr_hostname = node_match.group(2)
                curr_ncpu = int(node_match.group(3))
            elif queue_match:
                ts = int(queue_match.group(1))
                queue_name = queue_match.group(2)
                used = int(queue_match.group(4))    # b in a/b/c
                total = int(queue_match.group(5))   # c in a/b/c
                parsed_rows.append({
                    'timestamp': ts,
                    'hostname': curr_hostname,
                    'ncpu': curr_ncpu,
                    'queue': queue_name,
                    'cores_util': used,
                    'cores_total': total
                })
    # Convert to DataFrame
    df = pd.DataFrame(parsed_rows)
    if df.empty:
        print("df empty!")
        continue
    # Parse time, filter out unparseable
    df['time'] = pd.to_datetime(df['timestamp'], unit='s', errors='coerce')
    df = df.dropna(subset=['time'])
    df['date'] = df['time'].dt.date

    # Merge queue metadata
    df_meta = pd.merge(
        df,
        queue_info,
        left_on="queue",
        right_on="queuename",
        how='left'
    )[['date','queue','hostname','ncpu','cores_util','cores_total','queuetotal','class_util']]

    # GROUP: sum queue utilization per node & queue per day
    # If you want aggregate by queuetotal (superqueue) use 'queuetotal', otherwise by actual queue
    df_meta['cores_util'] = pd.to_numeric(df_meta['cores_util'], errors='coerce').fillna(0)
    agg = df_meta.groupby(['date', 'ncpu', 'queuetotal', 'class_util'])['cores_util'].sum().reset_index()
    
    # For cores_total, take the max for that queue for that day (should be same for all)
    header_daily = df_meta.groupby(['date', 'ncpu', 'queuetotal', 'class_util'])['cores_total'].max().reset_index()

    out_daily = pd.merge(agg, header_daily, on=['date','ncpu','queuetotal','class_util'], how='left')
    out_daily['util'] = out_daily['cores_util'] / out_daily['cores_total']
    result_daily = out_daily[['date', 'ncpu', 'queuetotal', 'cores_util', 'cores_total', 'util', 'class_util']]
    
    all_results.append(result_daily)

# Combine all months
if all_results:
    final = pd.concat(all_results, ignore_index=True)
    final.to_csv(output_csv, index=False)
    print(f"Combined daily utilization written to: {output_csv}")
else:
    print("No data found in the specified range.")