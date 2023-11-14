import pandas as pd
from sqlalchemy import create_engine, Integer, String, Text, Float, DECIMAL

#Importing the Data
data_url = "https://opendata.rhein-kreis-neuss.de/api/v2/catalog/datasets/rhein-kreis-neuss-flughafen-weltweit/exports/csv"
data = pd.read_csv(data_url, delimiter=';', on_bad_lines='skip')

#Assigning the Datatypes
column_types = {
    'column_1': Integer,
    'column_2': Text,
    'column_3': Text,
    'column_4': Text,
    'column_5': String,
    'column_6': String,
    'column_7': Float,
    'column_8': Float,
    'column_9': Integer,
    'column_10': Float,
    'column_11': String,
    'column_12': Text,
    'geo_punkt': DECIMAL
}

#SQLite Engine
engine = create_engine('sqlite:///airports.sqlite')

#Importing the data into SQLite
data.to_sql('airports', engine, if_exists="replace", index=False)
