import json
import pandas as pd
import numpy as np
import os 
import time
import re #regular expressions
from sqlalchemy import create_engine #enables connection to database
from config import db_password


# -----CONNECTIONS

#Postgres
connection_string = f'postgres://postgres:{db_password}@localhost:5432/movie_data'
engine = create_engine(connection_string)

# Relative path for data sources
resources_file_path = os.path.join("resources", 'wikipedia.movies.json')
data_file_path = os.path.join("data/")
#-----------------------------------------------------------------------------------------------

# -----EXTRACTIONS

# Wiki Movies Data
with open(resources_file_path, mode='r') as file:
    wiki_movies_raw = json.load(file)
# Convert the wiki-movies data to a Data Frame
wiki_movies_df = pd.DataFrame(wiki_movies_raw)

# Kaggle Data
kaggle_metadata_df = pd.read_csv(f'{data_file_path}movies_metadata.csv')

ratings_df = pd.read_csv(f'{data_file_path}ratings.csv')
#-----------------------------------------------------------------------------------------------


#Function takes 3 Dataframes
def Movies_ETL (wiki_movies_df , kaggle_metadata_df, ratings_df):



    return movies_df