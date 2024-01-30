import sqlite3
import pandas as pd
import urllib.request
import zipfile
import os

# Step 1: Download GTFS data
url = 'https://gtfs.rhoenenergie-bus.de/GTFS.zip'
gtfs_zip_path = 'GTFS.zip'
urllib.request.urlretrieve(url, gtfs_zip_path)

# Step 2: Extract stops.txt from the ZIP file
with zipfile.ZipFile(gtfs_zip_path, 'r') as zip_ref:
    zip_ref.extract('stops.txt')

# Step 3: Load stops.txt into a DataFrame
stops_df = pd.read_csv('stops.txt', usecols=['stop_id', 'stop_name', 'stop_lat', 'stop_lon', 'zone_id'])

# Step 4: Filter data for zone 2001 and validate data
stops_df = stops_df[stops_df['zone_id'] == 2001]
stops_df = stops_df[(stops_df['stop_lat'].between(-90, 90)) & (stops_df['stop_lon'].between(-180, 180))]

# Step 5: Drop rows with invalid data
stops_df.dropna(subset=['stop_name', 'stop_lat', 'stop_lon'], inplace=True)

# Step 6: Connect to SQLite database and create table
conn = sqlite3.connect('gtfs.sqlite')
cursor = conn.cursor()
cursor.execute('''
CREATE TABLE IF NOT EXISTS stops (
    stop_id TEXT,
    stop_name TEXT,
    stop_lat FLOAT,
    stop_lon FLOAT,
    zone_id BIGINT
)
''')

# Step 7: Write data into SQLite database
stops_df.to_sql('stops', conn, if_exists='replace', index=False, dtype={
    'stop_id': 'TEXT',
    'stop_name': 'TEXT',
    'stop_lat': 'FLOAT',
    'stop_lon': 'FLOAT',
    'zone_id': 'BIGINT'
})

# Step 8: Clean up and close connections
conn.commit()
conn.close()
os.remove('stops.txt')  # Clean up extracted file
os.remove(gtfs_zip_path)  # Clean up downloaded ZIP file

