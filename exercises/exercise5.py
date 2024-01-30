import pandas as pd
import sqlite3
import urllib.request
import zipfile
import os

gtfs_url = "https://gtfs.rhoenenergie-bus.de/GTFS.zip"
local_zip_file = "downloaded_gtfs.zip"
urllib.request.urlretrieve(gtfs_url, local_zip_file)

extracted_folder = "extracted_gtfs"
with zipfile.ZipFile(local_zip_file, 'r') as zip_file:
    zip_file.extractall(extracted_folder)
extracted_stops_file = os.path.join(extracted_folder, "stops.txt")

stops_df = pd.read_csv(extracted_stops_file)
stops_df = stops_df[['stop_id', 'stop_name', 'stop_lat', 'stop_lon', 'zone_id']]
stops_df = stops_df[stops_df['zone_id'] == 2001]
stops_df = stops_df[(stops_df['stop_lat'] >= -90) & (stops_df['stop_lat'] <= 90) & (stops_df['stop_lon'] >= -90) & (stops_df['stop_lon'] <= 90)]

database_connection = sqlite3.connect('gtfs.sqlite')
stops_df.to_sql('stops', database_connection, if_exists='replace', index=False, dtype={
    'stop_id': 'INTEGER',  
    'stop_name': 'TEXT',
    'stop_lat': 'FLOAT',  
    'stop_lon': 'FLOAT',
    'zone_id': 'INTEGER'  
})

database_connection.close()
