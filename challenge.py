#!/usr/bin/env python
# coding: utf-8

# #### Imports and variables for OS files

# In[1]:


#Import dependencies
import json
import pandas as pd
import numpy as np
import re
from config import db_password
import time
import sys
import logging
from sqlalchemy import create_engine


# In[2]:


#set up the files we'll use
file_dir = '/Users/rfcelorio/BerkeleyExtension/M8-ETL/Movies-ETL/'
wikipedia_data_file = f'{file_dir}wikipedia.movies.json'
kaggle_metadata_file = f'{file_dir}movies_metadata.csv'
ratings_file = f'{file_dir}ratings.csv'
log_file = f'{file_dir}etl.log'


# In[3]:


#log configuration
logger = logging.getLogger('movies-etl')
hdlr = logging.FileHandler(log_file, mode='w')
formatter = logging.Formatter('%(asctime)s %(levelname)s %(message)s')
hdlr.setFormatter(formatter)
logger.addHandler(hdlr) 
logger.setLevel(logging.INFO)
logger.info('### Starting ETL Process ###')


# #### Function to clean the wikipedia movies scrape

# In[4]:


def clean_movie(movie):
    try:
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
    except:
        logger.error(f'movies-etl had an error in clean_movie: {sys.exc_info()[0]}')


# #### Function to parse through dollar strings

# In[5]:


#function to parse dollars
def parse_dollars(s):
    try:
        # if s is not a string, return NaN
        if type(s) != str:
            return np.nan

        # if input is of the form $###.# million
        if re.match(r'\$\s*\d+\.?\d*\s*milli?on', s, flags=re.IGNORECASE):

            # remove dollar sign and " million"
            s = re.sub('\$|\s|[a-zA-Z]','', s)

            # convert to float and multiply by a million
            value = float(s) * 10**6

            # return value
            return value

        # if input is of the form $###.# billion
        elif re.match(r'\$\s*\d+\.?\d*\s*billi?on', s, flags=re.IGNORECASE):

            # remove dollar sign and " billion"
            s = re.sub('\$|\s|[a-zA-Z]','', s)

            # convert to float and multiply by a billion
            value = float(s) * 10**9

            # return value
            return value

        # if input is of the form $###,###,###
        elif re.match(r'\$\s*\d{1,3}(?:[,\.]\d{3})+(?!\s[mb]illion)', s, flags=re.IGNORECASE):

            # remove dollar sign and commas
            s = re.sub('\$|,','', s)

            # convert to float
            value = float(s)

            # return value
            return value

        # otherwise, return NaN
        else:
            return np.nan
    except:
        logger.error(f'movies-etl had an error in parse_dollars: {sys.exc_info()[0]}')


# #### Function to process the wikipedia file and return the expected DF

# In[6]:


#process the Wiki file

def process_wiki_movies(path_to_wiki_file):
    try:
        #Open wiki file and return serialized JSON
        with open(path_to_wiki_file, mode='r') as file:
            wiki_movies_raw = json.load(file)

        #Conver to DF
        wiki_movies_df = pd.DataFrame(wiki_movies_raw)
    
        #consolidate director column into list 
        wiki_movies = [movie for movie in wiki_movies_raw
                       if ('Director' in movie or 'Directed by' in movie)
                           and 'imdb_link' in movie
                           and 'No. of episodes' not in movie]
        
        #clean the movies & convert to df
        clean_movies = [clean_movie(movie) for movie in wiki_movies]
        wiki_movies_df = pd.DataFrame(clean_movies)

        #get a deduped imdb id so we can use it as an index
        wiki_movies_df['imdb_id'] = wiki_movies_df['imdb_link'].str.extract(r'(tt\d{7})')
        wiki_movies_df.drop_duplicates(subset='imdb_id', inplace=True)

        #thin it down
        wiki_columns_to_keep = [column for column in wiki_movies_df.columns if wiki_movies_df[column].isnull().sum() < len(wiki_movies_df) * 0.9]
        wiki_movies_df = wiki_movies_df[wiki_columns_to_keep]

    except:
         logger.error(f'movies-etl had an error in process_wiki_movies: {sys.exc_info()[0]}')

    
    #local function to clean money 
    def clean_money_col(df_column_name,new_column_name):
    
        #transform the column
        working_column = wiki_movies_df[df_column_name].dropna() #drop null
        working_column = working_column.apply(lambda x: ' '.join(x) if type(x) == list else x) #conver lists
        working_column = working_column.str.replace(r'\$.*[-—–](?![a-z])', '$', regex=True) #parse the - types
        working_column = working_column.str.replace(r'\[\d+\]\s*', '') #clean bracketed numbers
        
        #get dollars by regeex
        form_one = r'\$\s*\d+\.?\d*\s*[mb]illi?on'
        form_two = r'\$\s*\d{1,3}(?:[,\.]\d{3})+(?!\s[mb]illion)'    

        # drop from source
        wiki_movies_df[new_column_name] = working_column.str.extract(f'({form_one}|{form_two})', flags=re.IGNORECASE)[0].apply(parse_dollars)
        wiki_movies_df.drop(df_column_name, axis=1, inplace=True)
        return wiki_movies_df
    

    try:
        #process money columns
        clean_money_col('Box office','box_office')
        clean_money_col('Budget','budget')
    
        #Process Release Date 
        release_date = wiki_movies_df['Release date'].dropna().apply(lambda x: ' '.join(x) if type(x) == list else x)
        date_form_one = r'(?:January|February|March|April|May|June|July|August|September|October|November|December)\s[123]\d,\s\d{4}'
        date_form_two = r'\d{4}.[01]\d.[123]\d'
        date_form_three = r'(?:January|February|March|April|May|June|July|August|September|October|November|December)\s\d{4}'
        date_form_four = r'\d{4}'
        
        #add to DF and drop the old one
        wiki_movies_df['release_date'] = pd.to_datetime(release_date.str.extract(f'({date_form_one}|{date_form_two}|{date_form_three}|{date_form_four})')[0], infer_datetime_format=True)
        wiki_movies_df.drop('Release date', axis=1, inplace=True)
        
        #process time fields
        running_time = wiki_movies_df['Running time'].dropna().apply(lambda x: ' '.join(x) if type(x) == list else x)
        running_time_extract = running_time.str.extract(r'(\d+)\s*ho?u?r?s?\s*(\d*)|(\d+)\s*m')
        running_time_extract = running_time_extract.apply(lambda col: pd.to_numeric(col, errors='coerce')).fillna(0)
                
        #add to DF and drop the old one
        wiki_movies_df['running_time'] = running_time_extract.apply(lambda row: row[0]*60 + row[1] if row[2] == 0 else row[2], axis=1)
        wiki_movies_df.drop('Running time', axis=1, inplace=True)
        
        return wiki_movies_df
    
    except:
         logger.error(f'movies-etl had an error in process_wiki_movies: {sys.exc_info()[0]}')


# #### Function to encapsulate the kaggle file transformations

# In[7]:


#Process the kaggle data
def process_kaggle_movies(path_to_kaggle_file):
    try:
        #Open the file
        kaggle_metadata = pd.read_csv(path_to_kaggle_file)
        #remove adult films
        kaggle_metadata = kaggle_metadata[kaggle_metadata['adult'] == 'False'].drop('adult',axis='columns')

        #build DF columns
        kaggle_metadata['video'] = kaggle_metadata['video'] == 'True'
        kaggle_metadata['budget'] = kaggle_metadata['budget'].astype(int)
        kaggle_metadata['id'] = pd.to_numeric(kaggle_metadata['id'], errors='raise')
        kaggle_metadata['popularity'] = pd.to_numeric(kaggle_metadata['popularity'], errors='raise')
        kaggle_metadata['release_date'] = pd.to_datetime(kaggle_metadata['release_date'])

        return kaggle_metadata
    except:
        logger.error(f'movies-etl had an error in process_kaggle_movies: {sys.exc_info()[0]}')


# In[8]:


#Process the ratings data
def process_ratings(path_to_ratings_file):
    try:
        #Open the file
        ratings = pd.read_csv(f'{file_dir}ratings.csv')
        #fix the timestamp
        ratings['timestamp'] = pd.to_datetime(ratings['timestamp'], unit='s')
        #Group ratings by movieid
        rating_counts = ratings.groupby(['movieId','rating'], as_index=False).count()                     .rename({'userId':'count'}, axis=1)                     .pivot(index='movieId',columns='rating', values='count')
        rating_counts.columns = ['rating_' + str(col) for col in rating_counts.columns]
        return rating_counts
    except:
        logger.error(f'movies-etl had an error in process_ratings: {sys.exc_info()[0]}')


# In[9]:


#load the ratings data in chunks
def load_ratings(path_to_ratings_file,db_string):
    try:
        engine = create_engine(db_string)
        rows_imported = 0
        # get the start_time from time.time()
        start_time = time.time()
        for data in pd.read_csv(path_to_ratings_file, chunksize=1000000):
            logger.info(f'importing rows {rows_imported:,} to {rows_imported + len(data):,}...')
            data.to_sql(name='ratings', con=engine, if_exists='append')
            rows_imported += len(data)
            
            # add elapsed time to final print out
            logger.info(f'Done. {time.time() - start_time} total seconds elapsed')
             
    except:
        logger.error(f'movies-etl had an error in load_ratings: {sys.exc_info()[0]}')
    


# ## Challenge Function

# In[11]:


def main_movies_etl(path_to_wiki_file, path_to_kaggle_file, path_to_ratings_file):
    try:
        # call the functions to process Wiki and Kaggle files
        try:
            wiki_movies_df = process_wiki_movies(path_to_wiki_file)
            logger.info('Processed wiki file')
        except:
            logger.error(f'movies-etl had an error running process_wiki_movies: {sys.exc_info()[0]}')
        
        try:
            kaggle_metadata = process_kaggle_movies(path_to_kaggle_file)
            logger.info('Processed kaggle file')
        except:
            logger.error(f'movies-etl had an error running process_kaggle_movies: {sys.exc_info()[0]}')
        
        # Merge data sets to further transform
        movies_df = pd.merge(wiki_movies_df, kaggle_metadata, on='imdb_id', suffixes=['_wiki','_kaggle'])
        #drop some garbage
        movies_df = movies_df.drop(movies_df[(movies_df['release_date_wiki'] > '1996-01-01') & (movies_df['release_date_kaggle'] < '1965-01-01')].index)
        #convert list colums into tuples
        movies_df['Language'].apply(lambda x: tuple(x) if type(x) == list else x).value_counts(dropna=False)
        #drop columns we dont need
        movies_df.drop(columns=['title_wiki','release_date_wiki','Language','Production company(s)'], inplace=True)

        #through analysis we determined the kaggle data is more complete than the wiki. This function fills in kaggle data from the wiki column then drops the wiki column.
        def fill_missing_kaggle_data(df, kaggle_column, wiki_column):
            df[kaggle_column] = df.apply(
                lambda row: row[wiki_column] if row[kaggle_column] == 0 else row[kaggle_column]
                , axis=1)
            df.drop(columns=wiki_column, inplace=True)

        fill_missing_kaggle_data(movies_df, 'runtime', 'running_time')
        fill_missing_kaggle_data(movies_df, 'budget_kaggle', 'budget_wiki')
        fill_missing_kaggle_data(movies_df, 'revenue', 'box_office')

        #reorder and rename columns
        movies_df = movies_df[['imdb_id','id','title_kaggle','original_title','tagline','belongs_to_collection','url','imdb_link',
                           'runtime','budget_kaggle','revenue','release_date_kaggle','popularity','vote_average','vote_count',
                           'genres','original_language','overview','spoken_languages','Country',
                           'production_companies','production_countries','Distributor',
                           'Producer(s)','Director','Starring','Cinematography','Editor(s)','Writer(s)','Composer(s)','Based on'
                          ]]

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

        #call function to return the grouped ratings
        rating_counts = process_ratings(ratings_file)
        # join the ratings
        movies_with_ratings_df = pd.merge(movies_df, rating_counts, left_on='kaggle_id', right_index=True, how='left')
        #fill the nulls
        movies_with_ratings_df[rating_counts.columns] = movies_with_ratings_df[rating_counts.columns].fillna(0)
        
        #load movies to db
        db_string = f"postgres://postgres:{db_password}@127.0.0.1:5432/movie_data"
        engine = create_engine(db_string)
        
        try:
            #delete data without dropping
            sql_truncate = 'TRUNCATE TABLE public.movies, public.ratings CASCADE;'
            engine.execute(sql_truncate)
            logger.info('Truncated tables')
        except:
            logger.error('FAILED truncate tables')
            
        try:
            #drop and recreate the existing table before loading
            movies_with_ratings_df.to_sql(name='movies', con=engine, if_exists='append')
            logger.info('Finished loading movies')
        except:
            logger.error('Failed loading movies')
        
        try:
            #load the ratings file in chunks
            load_ratings(ratings_file, db_string) 
            logger.info('Finished loading ratings')
        except:
            logger.error('FAILED loading ratings')
        
        logger.info('### END ETL Process ###')
    except:
         logger.error(f'movies-etl had an error in main_movies_etl: {sys.exc_info()[0]}')
    


# In[12]:


movies_with_ratings_df = main_movies_etl(wikipedia_data_file,kaggle_metadata_file,ratings_file)


# In[ ]:




