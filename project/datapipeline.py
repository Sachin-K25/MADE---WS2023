import requests
import pandas as pd
from io import StringIO
import sqlite3
import os

#Importing the data
munich_population_durofresidence = "https://data.europa.eu/data/visualisation/?file=https%3A%2F%2Fopendata.muenchen.de%2Fdataset%2F8a2e197b-69ff-4456-aff1-48763cb5dea3%2Fresource%2F773a81c6-1467-4c4a-adde-329776f4b44e%2Fdownload%2Findikat_bevoelkerung_wohndauer_muenchen_240723.csv"
munich_parking_garages = "https://opendata.muenchen.de/dataset/addaa7d4-40be-4621-846e-c5358cbe3f26/resource/e0e0e4e1-1b25-4c04-a0ea-cf9cc8335c57/download/230907places.csv"

population_response = requests.get(munich_population_durofresidence)
parking_garages_response= requests.get(munich_parking_garages)

population_data = population_response.text
parking_data = parking_garages_response.text

#Replacing the missing values
population_df = pd.read_csv(StringIO(population_data),delimiter=';', on_bad_lines='skip')
population_df = population_df.fillna(0) 

parking_df = pd.read_csv(StringIO(parking_data))
parking_df = parking_df.fillna(0) 

#Defining the DB path
data_dir = '../data'
if not os.path.exists(data_dir):
    os.makedirs(data_dir)


db_path = os.path.join(data_dir, 'dataset2.db')

#pushing the files to the DB
conn = sqlite3.connect(db_path)
population_df.to_sql('population_table', conn, if_exists='replace', index=False)
parking_df.to_sql('parking-garages_table', conn, if_exists='replace', index=False)
conn.close()

print("Data pipeline created")
