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


# -----EXTRACTIONS (this may need to be in the function?)

# Wiki Movies Data
with open(resources_file_path, mode='r') as file:
    wiki_movies_raw = json.load(file)
# Convert the wiki-movies data to a Data Frame
wiki_movies_df = pd.DataFrame(wiki_movies_raw)

# Kaggle Data
kaggle_metadata_df = pd.read_csv(f'{data_file_path}movies_metadata.csv')

ratings_df = pd.read_csv(f'{data_file_path}ratings.csv')
#-----------------------------------------------------------------------------------------------


# -----ETL FUNCTION
#Function takes 3 Dataframes
def Movies_ETL (wiki_movies_df, kaggle_metadata_df, ratings_df):


    # Filter raw wikipedia data to just movies
    # Only those entries that have a director, an IMDb link and NO episodes gives us movies
    wiki_movies = [movie for movie in wiki_movies_raw
               if ('Director' in movie or 'Directed by' in movie)
                   and 'imdb_link' in movie
                   and 'No. of episodes' not in movie]

    #convert list to DF
    wiki_movies_df = pd.DataFrame(wiki_movies)


    # version2 of clean_movie
    # There are quite a few columns with slightly different names but the same data, such as “Directed by” and “Director.”

    # Loop through a list of all alternative title keys 
    # & Check if the current key exists in the movie object
    # If so, remove the key-value pair and add to the alternative titles dictionary
    # After looping through every key, add the alternative titles dict to the movie object
    def clean_movie(movie):
        movie = dict(movie) #create a non-destructive copy
        alt_titles = {}
        # combine alternate titles into one list
        for key in ['Also known as','Arabic','Cantonese','Chinese','French',
                    'Hangul','Hebrew','Hepburn','Japanese','Literally',
                    'Mandarin','McCune-Reischauer','Original title','Polish',
                    'Revised Romanization','Romanized','Russian',
                    'Simplified','Traditional','Yiddish']:
            if key in movie:
                alt_titles[key] = movie[key]
                movie.pop(key)
        if len(alt_titles) > 0:
            movie['alt_titles'] = alt_titles

        # merge column names
        def change_column_name(old_name, new_name):
            if old_name in movie:
                movie[new_name] = movie.pop(old_name)
        change_column_name('Adaptation by', 'Writer(s)')
        change_column_name('Country of origin', 'Country')
        change_column_name('Directed by', 'Director')
        change_column_name('Distributed by', 'Distributor')
        change_column_name('Edited by', 'Editor(s)')
        change_column_name('Length', 'Running time')
        change_column_name('Original release', 'Release date')
        change_column_name('Music by', 'Composer(s)')
        change_column_name('Produced by', 'Producer(s)')
        change_column_name('Producer', 'Producer(s)')
        change_column_name('Productioncompanies ', 'Production company(s)')
        change_column_name('Productioncompany ', 'Production company(s)')
        change_column_name('Released', 'Release Date')
        change_column_name('Release Date', 'Release date')
        change_column_name('Screen story by', 'Writer(s)')
        change_column_name('Screenplay by', 'Writer(s)')
        change_column_name('Story by', 'Writer(s)')
        change_column_name('Theme music composer', 'Composer(s)')
        change_column_name('Written by', 'Writer(s)')

        return movie

    # List comprehension to clean wiki_movies and recreate wiki_movies_df
    clean_movies = [clean_movie(movie) for movie in wiki_movies]
    wiki_movies_df = pd.DataFrame(clean_movies)


    # Remove duplicate rows leveraging a regular expression
    wiki_movies_df['imdb_id'] = wiki_movies_df['imdb_link'].str.extract(r'(tt\d{7})')

    wiki_movies_df.drop_duplicates(subset='imdb_id', inplace=True)

    # get the count of null values for each column is using a list comprehension
    # reduce down to columns that have <90% nulls
    wiki_columns_to_keep = [column for column in wiki_movies_df.columns if wiki_movies_df[column].isnull().sum() < len(wiki_movies_df) * 0.9]
    wiki_movies_df = wiki_movies_df[wiki_columns_to_keep]

    ############################
    # Convert and Parse the Data


    # BOX OFFICE DATA

    # Drop rows that are missing box office data
    box_office = wiki_movies_df['Box office'].dropna() 

    # There are quite a few data points that are stored as lists
    # There is a join() string method that concatenates list items into one string;
    box_office = box_office.apply(lambda x: ' '.join(x) if type(x) == list else x)

    form_one = r'\$\s*\d+\.?\d*\s*[mb]illi?on'
    form_two = r'\$\s*\d{1,3}(?:[,\.]\d{3})+(?!\s[mb]illion)'
    box_office = box_office.str.replace(r'\$.*[-—–](?![a-z])', '$', regex=True)

    # Function to parse dollars
    def parse_dollars(s):
        # if s is not a string, return NaN
        if type(s) != str:
            return np.nan

        # if input is of the form $###.# million
        if re.match(r'\$\s*\d+\.?\d*\s*milli?on', s, flags=re.IGNORECASE):

            # remove dollar sign and " million"
            s = re.sub(r'\$|\s|[a-zA-Z]','', s)

            # convert to float and multiply by a million
            value = float(s) * 10**6

            # return value
            return value

        # if input is of the form $###.# billion
        elif re.match(r'\$\s*\d+\.?\d*\s*billi?on', s, flags=re.IGNORECASE):

            # remove dollar sign and " billion"
            s = re.sub(r'\$|\s|[a-zA-Z]','', s)

            # convert to float and multiply by a billion
            value = float(s) * 10**9

            # return value
            return value

        # if input is of the form $###,###,###
        elif re.match(r'\$\s*\d{1,3}(?:[,\.]\d{3})+(?!\s[mb]illion)', s, flags=re.IGNORECASE):

            # remove dollar sign and commas
            s = re.sub(r'\$|,','', s)

            # convert to float
            value = float(s)

            # return value
            return value

        # otherwise, return NaN
        else:
            return np.nan

    # Parse the box office values to numeric values.
    # First, extract the values from box_office using str.extract. 
    # Then we'll apply parse_dollars to the first column in the DataFrame returned by str.extract, 
    wiki_movies_df['box_office'] = box_office.str.extract(f'({form_one}|{form_two})', flags=re.IGNORECASE)[0].apply(parse_dollars)

    wiki_movies_df.drop('Box office', axis=1, inplace=True)


    # BUDGET DATA
    budget = wiki_movies_df['Budget'].dropna()

    # convert lists to strings
    budget = budget.map(lambda x: ' '.join(x) if type(x) == list else x)

    # remove any values between a dollar sign and a hyphen (for budgets given in ranges)
    budget = budget.str.replace(r'\$.*[-—–](?![a-z])', '$', regex=True)

    # Decision made to ignore the remaining 30 budgets with issues
    budget = budget.str.replace(r'\[\d+\]\s*', '')

    # Parse
    wiki_movies_df['budget'] = budget.str.extract(f'({form_one}|{form_two})', flags=re.IGNORECASE)[0].apply(parse_dollars)

    # PARSE RELEASE DATA
    # First, make a variable that holds the non-null values of Release date in the DataFrame, converting lists to strings:
    release_date = wiki_movies_df['Release date'].dropna().apply(lambda x: ' '.join(x) if type(x) == list else x)

    date_form_one = r'(?:January|February|March|April|May|June|July|August|September|October|November|December)\s[123]\d,\s\d{4}'
    date_form_two = r'\d{4}.[01]\d.[123]\d'
    date_form_three = r'(?:January|February|March|April|May|June|July|August|September|October|November|December)\s\d{4}'
    date_form_four = r'\d{4}'

    # extract the dates
    release_date.str.extract(f'({date_form_one}|{date_form_two}|{date_form_three}|{date_form_four})', flags=re.IGNORECASE)

    # Use the built-in to_datetime() method in Pandas.
    wiki_movies_df['release_date'] = pd.to_datetime(release_date.str.extract(f'({date_form_one}|{date_form_two}|{date_form_three}|{date_form_four})')[0], infer_datetime_format=True)

    # PARSE RUNNING TIME
    running_time = wiki_movies_df['Running time'].dropna().apply(lambda x: ' '.join(x) if type(x) == list else x)

    running_time_extract = running_time.str.extract(r'(\d+)\s*ho?u?r?s?\s*(\d*)|(\d+)\s*m')

    # need to convert them to numeric values. Because we may have captured empty strings, 
    # we’ll use the to_numeric() method and set the errors argument to 'coerce'. 
    # Coercing the errors will turn the empty strings into Not a Number (NaN), 
    # then we can use fillna() to change all the NaNs to zeros.
    running_time_extract = running_time_extract.apply(lambda col: pd.to_numeric(col, errors='coerce')).fillna(0)

    # Now we can apply a function that will convert the hour capture groups and minute capture groups to minutes 
    # if the pure minutes capture group is zero, and save the output to wiki_movies_df
    wiki_movies_df['running_time'] = running_time_extract.apply(lambda row: row[0]*60 + row[1] if row[2] == 0 else row[2], axis=1)

    wiki_movies_df.drop('Running time', axis=1, inplace=True)


    ############################
    # Clean the Kaggle Data

    # Drop the adult rows - there is some bad data but also they aren't appropriate for the hackathon
    kaggle_metadata_df = kaggle_metadata_df[kaggle_metadata_df['adult'] == 'False'].drop('adult',axis='columns')

    kaggle_metadata_df['video'] = kaggle_metadata_df['video'] == 'True'

    # For the numeric columns, we can just use the to_numeric() method from Pandas. 
    # We’ll make sure the errors= argument is set to 'raise', 
    # so we’ll know if there’s any data that can’t be converted to numbers.

    kaggle_metadata_df['budget'] = kaggle_metadata_df['budget'].astype(int)
    kaggle_metadata_df['id'] = pd.to_numeric(kaggle_metadata_df['id'], errors='raise')
    kaggle_metadata_df['popularity'] = pd.to_numeric(kaggle_metadata_df['popularity'], errors='raise')

    kaggle_metadata_df['release_date'] = pd.to_datetime(kaggle_metadata_df['release_date'])

    # RATINGS DATA
    # These dates don’t seem outlandish—the years are within expected bounds, 
    # and there appears to be some consistency from one entry to the next. 
    # Since the output looks reasonable, assign it to the timestamp column.

    ratings_df['timestamp'] = pd.to_datetime(ratings_df['timestamp'], unit='s')


    # Merge Wikipedia and Kaggle Metadata

    # Print out a list of the columns so we can identify which ones are redundant
    movies_df = pd.merge(wiki_movies_df, kaggle_metadata_df, on='imdb_id', suffixes=['_wiki','_kaggle'])


    # Table summarizes merge decision with high level justifications
    # Competing data:
    # Wiki                     Movielens                Resolution
    #--------------------------------------------------------------------------
    # title_wiki               title_kaggle             Drop Wikipedia (both very consistent) (kaggle slightly more consistent)
    # running_time             runtime                  Keep Kaggle; fill in zeros with Wikipedia data. (wiki data had more outliers on histogram)
    # budget_wiki              budget_kaggle            Keep Kaggle; fill in zeros with Wikipedia data. (wiki data had more outliers on histogram)
    # box_office               revenue                  Keep Kaggle; fill in zeros with Wikipedia data.
    # release_date_wiki        release_date_kaggle      Drop Wikipedia (kaggle data clean while wiki has missing data)
    # Language                 original_language        Drop Wikipedia (kaggle is cleaner & more consistent)
    # Production company(s)    production_companies     Drop Wikipedia

    # First, we’ll drop the title_wiki, release_date_wiki, Language, and Production company(s) columns
    movies_df.drop(columns=['title_wiki','release_date_wiki','Language','Production company(s)'], inplace=True)

    # Fills in missing data for a column pair and then drops the redundant column
    def fill_missing_kaggle_data(df, kaggle_column, wiki_column):
        df[kaggle_column] = df.apply(
            lambda row: row[wiki_column] if row[kaggle_column] == 0 else row[kaggle_column]
            , axis=1)
        df.drop(columns=wiki_column, inplace=True)

    # Now we can run the function for the three column pairs that we decided to fill in zeros.
    fill_missing_kaggle_data(movies_df, 'runtime', 'running_time')
    fill_missing_kaggle_data(movies_df, 'budget_kaggle', 'budget_wiki')
    fill_missing_kaggle_data(movies_df, 'revenue', 'box_office')

    #check that there aren’t any columns with only one value, since that doesn’t really provide any information
    # Don’t forget, we need to convert lists to tuples for value_counts() to work.

    for col in movies_df.columns:
        lists_to_tuples = lambda x: tuple(x) if type(x) == list else x
        value_counts = movies_df[col].apply(lists_to_tuples).value_counts(dropna=False)
        num_values = len(value_counts)
        if num_values == 1:
            movies_df[col].value_counts(dropna=False)

    # reorder coloumns
    movies_df = movies_df.loc[:, ['imdb_id','id','title_kaggle','original_title','tagline','belongs_to_collection','url','imdb_link', 
                            'runtime','budget_kaggle','revenue','release_date_kaggle','popularity','vote_average','vote_count',
                            'genres','original_language','overview','spoken_languages','Country',
                            'production_companies','production_countries','Distributor',
                            'Producer(s)','Director','Starring','Cinematography','Editor(s)','Writer(s)','Composer(s)','Based on'
                            ]]

    # update names
    movies_df.rename({'id':'kaggle_id',
                    'title_kaggle':'title',
                    'url':'wikipedia_url',
                    'budget_kaggle':'budget',
                    'release_date_kaggle':'release_date',
                    'Country':'country',
                    'Distributor':'distributor',
                    'Producer(s)':'producers',
                    'Director':'director',
                    'Starring':'starring',
                    'Cinematography':'cinematography',
                    'Editor(s)':'editors',
                    'Writer(s)':'writers',
                    'Composer(s)':'composers',
                    'Based on':'based_on'
                    }, axis='columns', inplace=True)


    # rating data
    rating_counts_df = ratings_df.groupby(['movieId','rating'], as_index=False).count()

    #The choice of renaming “userId” to “count” is arbitrary. 
    # Both “userId” and “timestamp” have the same information, so we could use either one.
    rating_counts_df = ratings_df.groupby(['movieId','rating'], as_index=False).count() \
                    .rename({'userId':'count'}, axis=1)     

    #pivot this data so that movieId is the index, 
    # the columns will be all the rating values, and the rows will be the counts for each rating value.
    rating_counts_df = ratings_df.groupby(['movieId','rating'], as_index=False).count() \
                    .rename({'userId':'count'}, axis=1) \
                    .pivot(index='movieId',columns='rating', values='count')

    # rename the columns so they’re easier to understand. 
    # We’ll prepend rating_ to each column with a list comprehension:
    rating_counts_df.columns = ['rating_' + str(col) for col in rating_counts_df.columns]

    # use a left merge, since we want to keep everything in movies_df
    movies_with_ratings_df = pd.merge(movies_df, rating_counts_df, left_on='kaggle_id', right_index=True, how='left')

    # Finally, because not every movie got a rating for each rating level, 
    # there will be missing values instead of zeros. We have to fill those in ourselves, like this:
    movies_with_ratings_df[rating_counts_df.columns] = movies_with_ratings_df[rating_counts_df.columns].fillna(0)

    # 

    # To save the movies_df DataFrame to a SQL table, 
    try:
        movies_df.to_sql(name='movies', con=engine, if_exists='replace')
    except Exception as ex: 
        print(type(ex).__name__)
        print("Error loading to Postgres")
        
    return 
#-----------------------------------------------------------------------------------------------


# *****Test the function*****
Movies_ETL (wiki_movies_df, kaggle_metadata_df, ratings_df)