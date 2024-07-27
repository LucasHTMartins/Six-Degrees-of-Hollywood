'''
Main module responsible for retrieving IMDB raw files, cleaning data,
and updating databases
'''

import csv
import psycopg2
import requests
import shutil
import gzip


def check_database(database_connection) -> bool:
    '''Checks if database tables exist

    Parameters
    ----------
    database_connection
        COnnection to the database

    Returns
    -------
    True
        If all tables exist

    Raises
    -------
    ConnectionError
        If any table is not found
    '''

    con = database_connection
    cur = con.cursor()

    table_names = ['people', 'stars', 'movies', 'ratings']

    for table_name in table_names:
        cur.execute('''SELECT EXISTS (
            SELECT 1 FROM information_schema.tables
            WHERE table_name = %s);
        ''', (table_name,))

        if cur.fetchone()[0] == False:
            raise ConnectionError("Mandatory database tables not found.")
    return True


def get_files():
    '''Downloads and extracts latest Non-commercial dataset from IMDB

    Prints updates to the console since process can take a while

    Parameters
    ----------
    None

    Returns
    -------
    None

    Side Effects
    -------
    Downloads, saves, and extracts, four tsv.gz files

    Raises
    -------
    ConnectionError
        IF response code is not 200
    '''

    response = requests.get('https://datasets.imdbws.com/name.basics.tsv.gz')
    if response.status_code != 200:
        raise ConnectionError('Connection to IMDb for database download failed...')

    with open('name.basics.tsv.gz', 'wb') as file:
        file.write(response.content)
    print('Names file downloaded sucessfully')

    with gzip.open('name.basics.tsv.gz', 'rt') as z_file:
        with open('names.tsv', 'wt') as d_file:
            shutil.copyfileobj(z_file, d_file)
    print('Names file extracted sucessfully')


    response = requests.get('https://datasets.imdbws.com/title.basics.tsv.gz')
    if response.status_code != 200:
        raise ConnectionError('Connection to IMDb for database download failed...')

    with open('title.basics.tsv.gz', 'wb') as file:
        file.write(response.content)

    print('Movies file downloaded sucessfully')

    with gzip.open('title.basics.tsv.gz', 'rt') as z_file:
        with open('movies.tsv', 'wt') as d_file:
            shutil.copyfileobj(z_file, d_file)
    print('Movies file extracted sucessfully')


    response = requests.get('https://datasets.imdbws.com/title.principals.tsv.gz')
    if response.status_code != 200:
        raise ConnectionError('Connection to IMDb for database download failed...')

    with open('title.principals.tsv.gz', 'wb') as file:
        file.write(response.content)
    print('Stars file downloaded sucessfully')

    with gzip.open('title.principals.tsv.gz', 'rt') as z_file:
        with open('stars.tsv', 'wt') as d_file:
            shutil.copyfileobj(z_file, d_file)
    print('Stars file extracted sucessfully')


    response = requests.get('https://datasets.imdbws.com/title.ratings.tsv.gz')
    if response.status_code != 200:
        raise ConnectionError('Connection to IMDb for database download failed...')

    with open('title.ratings.tsv.gz', 'wb') as file:
        file.write(response.content)
    print('Ratings file downloaded sucessfully')

    with gzip.open('title.ratings.tsv.gz', 'rt') as z_file:
        with open('ratings.tsv', 'wt') as d_file:
            shutil.copyfileobj(z_file, d_file)
    print('Ratings file extracted sucessfully')


def make_movies(connection):
    '''Creates the movies table

    Prints updates to the console every 100.000 entries since process can take a while

    Parameters
    ----------
    connection
        Cursor to the database

    Returns
    -------
    None

    Side Effects
    -------
    Creates table movies and loads data
    '''

    con = connection
    cur = con.cursor()

    cur.execute('DROP TABLE IF EXISTS movies CASCADE;')

    cur.execute('''CREATE TABLE movies (
        id INTEGER PRIMARY KEY,
        title TEXT NOT NULL,
        year INTEGER,
        title_type TEXT,
        is_adult INTEGER,
        runtime INTEGER,
        genres TEXT

    );''')

    count = 0
    with open('movies.tsv', 'r') as f:
        reader = csv.DictReader(f, delimiter='\t')

        for row in reader:

            id = int(row['tconst'][2:])
            title = row['primaryTitle']

            if row['startYear'].isdigit():
                year = int(row['startYear'])
            else:
                year = None

            if row['titleType'] == '\\N':
                title_type = None
            else:
                title_type = row['titleType']

            if row['isAdult'].isdigit():
                is_adult = int(row['isAdult'])
            else:
                is_adult = None

            if row['runtimeMinutes'].isdigit():
                runtime = int(row['runtimeMinutes'])
            else:
                runtime = None

            if row['genres'] == '\\N':
                genres = None
            else:
                genres = row['genres']

            insert = '''
                INSERT INTO movies (id, title, year, title_type, is_adult, runtime, genres)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                '''
            cur.execute(insert, (id, title, year, title_type, is_adult, runtime, genres))

            count += 1
            if count % 100000 == 0:
                count_str = f'{count:,}'
                print(f'Inserted {count_str} records into movies')
                con.commit()
    con.commit()


def make_ratings(connection):
    '''Creates the ratings table

    Prints updates to the console every 100.000 entries since process can take a while

    Parameters
    ----------
    connection
        Cursor to the database

    Returns
    -------
    None

    Side Effects
    -------
    Creates table ratings and loads data
    '''

    con = connection
    cur = con.cursor()

    cur.execute('DROP TABLE IF EXISTS ratings;')

    cur.execute('''CREATE TABLE ratings (
        movie_id INTEGER REFERENCES movies (id) ON DELETE CASCADE,
        average NUMERIC,
        num_votes INTEGER
    );''')

    count = 0
    with open('ratings.tsv', 'r') as f:
        reader = csv.DictReader(f, delimiter='\t')

        for row in reader:

            movie_id = int(row['tconst'][2:])

            cur.execute('SELECT id FROM movies WHERE id = %s', (movie_id,))
            result = cur.fetchone()

            if result is None:
                print(f'Skipping row with movie_id {movie_id}')
                continue

            if row['averageRating'] == '\\N':
                average = None
            else:
                average = float(row['averageRating'])

            if row['numVotes'] == '\\N':
                num_votes = None
            else:
                num_votes = int(row['numVotes'])

            insert = '''
                INSERT INTO ratings (movie_id, average, num_votes)
                VALUES (%s, %s, %s)
                '''
            cur.execute(insert, (movie_id, average, num_votes))

            count += 1
            if count % 100000 == 0:
                count_str = f'{count:,}'
                print(f'Inserted {count_str} records into ratings')
                con.commit()
    con.commit()

def make_people(connection):
    '''Creates the people table

    Prints updates to the console every 100.000 entries since process can take a while

    Parameters
    ----------
    connection
        Cursor to the database

    Returns
    -------
    None

    Side Effects
    -------
    Creates table people and loads data
    '''

    con = connection
    cur = con.cursor()

    cur.execute('DROP TABLE IF EXISTS people CASCADE;')

    cur.execute('''CREATE TABLE people (
        id INTEGER PRIMARY KEY,
        name TEXT,
        birth INTEGER,
        death INTEGER,
        famous_movies TEXT
    );''')

    count = 0
    with open('names.tsv', 'r') as f:
        reader = csv.DictReader(f, delimiter='\t')

        for row in reader:

            id = int(row['nconst'][2:])
            name = row['primaryName']
            birth = int(row['birthYear']) if row['birthYear'] != '\\N' else None
            death = int(row['deathYear']) if row['deathYear'] != '\\N' else None
            famous_movies = row['knownForTitles'] if row['knownForTitles'] != '\\N' else None

            insert = '''INSERT INTO people (id, name, birth, death, famous_movies)
                VALUES (%s, %s, %s, %s, %s)
                '''
            cur.execute(insert, (id, name, birth, death, famous_movies))

            count += 1
            if count % 100000 == 0:
                count_str = f'{count:,}'
                print(f'Inserted {count_str} records into people')
                con.commit()
    con.commit()

def make_stars(connection):
    '''Creates the stars table

    Prints updates to the console every 100.000 entries since process can take a while

    Parameters
    ----------
    connection
        Cursor to the database

    Returns
    -------
    None

    Side Effects
    -------
    Creates stars movies and loads data
    '''

    con = connection
    cur = con.cursor()

    cur.execute('DROP TABLE IF EXISTS stars;')

    cur.execute('''CREATE TABLE stars (
        person_id INTEGER REFERENCES people (id) ON DELETE CASCADE,
        movie_id INTEGER REFERENCES movies (id) ON DELETE CASCADE,
        category TEXT,
        UNIQUE (person_id, movie_id)
    );''')

    count = 0
    with open('stars.tsv', 'r') as f:
        reader = csv.DictReader(f, delimiter='\t')

        for row in reader:
            person_id = int(row['nconst'][2:])
            movie_id = int(row['tconst'][2:])
            category = row['category']

            cur.execute('SELECT id FROM people WHERE id = %s', (person_id,))
            result = cur.fetchone()

            if result is None:
                print(f'Skipping row with person_id {person_id}')
                continue

            cur.execute('SELECT id FROM movies WHERE id = %s', (movie_id,))
            result = cur.fetchone()

            if result is None:
                print(f'Skipping row with movie_id {movie_id}')
                continue

            insert = '''
                INSERT INTO stars (person_id, movie_id, category)
                VALUES (%s, %s, %s)
                ON CONFLICT (person_id, movie_id) DO NOTHING
            '''

            cur.execute(insert, (person_id, movie_id, category))

            count += 1
            if count % 100000 == 0:
                count_str = f'{count:,}'
                print(f'Inserted {count_str} records into stars')
                con.commit()
        con.commit()
    con.commit()

def create_indexes(connection):
    '''Checks the existence of, and if necessary creates, indexes

    Parameters
    ----------
    connection
        Cursor to the database

    Returns
    -------
    None

    Side Effects
    -------
    Creates up to 5 indexes on PostgreSQL
    '''

    con = connection
    cur = con.cursor()
    cur.execute('CREATE INDEX IF NOT EXISTS ratings_id_index ON ratings (movie_id);')
    cur.execute('CREATE INDEX IF NOT EXISTS movies_id_index ON movies (id);')
    cur.execute('CREATE INDEX IF NOT EXISTS people_id_index ON people (id);')
    cur.execute('CREATE INDEX IF NOT EXISTS stars_movies_id_index ON stars (movie_id);')
    cur.execute('CREATE INDEX IF NOT EXISTS stars_people_id_index ON stars (person_id);')
    con.commit()


def clean_data(connection):
    '''Deletes information that detracts from the projects goals

    For more information see README section 2. Cleaning data

    Parameters
    ----------
    connection
        Cursor to the database

    Returns
    -------
    None

    Side Effects
    -------
    Deletes a large amount of data from the database
    '''

    con = connection
    cur = con.cursor()

    cur.execute('DELETE FROM movies WHERE is_adult = 1')

    cur.execute('''DELETE FROM movies
        WHERE title_type
        NOT IN ('movie', 'short', 'tvSeries', 'tvMiniSeries')
    ;''')

    cur.execute('''DELETE FROM movies
        WHERE id IN (
            SELECT id
            FROM movies
            LEFT JOIN ratings ON id = movie_id
            WHERE num_ratings IS NULL
            OR num_ratings < 20
    );''')

    cur.execute('''DELETE FROM movies
        WHERE id IN (
            SELECT id
            FROM movies
            WHERE genres LIKE '%News%'
            OR genres LIKE '%Talk-Show%'
            OR genres LIKE '%Reality-TV%'
            OR genres LIKE '%Adult%'
    );''')

    cur.execute('''DELETE FROM people
        WHERE id IN (
            SELECT id
            FROM people
            LEFT JOIN stars ON id = person_id
            WHERE movie_id IS NULL
    );''')



if __name__ == '__main__':
    pass

