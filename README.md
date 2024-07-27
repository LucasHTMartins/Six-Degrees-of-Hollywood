# Six-Degrees-of-Hollywood

Six Degrees of Hollywood - with IMDB data, TMDB API, and Pillow poster creation

1. Summary
2. Data Cleaning
3. Requirements - How to set-up your environment
4. Tutorial - How to use main.py
5. Future improvements




1. Summary

The idea for this project comes from a variation of a concept which
states that: any two given people, are separated at most by six or fewer
social connections. A common variation (Six Degrees of Kevin Bacon)
states that any other actor is separated from Kevin Bacon by the same
six or fewer social connections

This project uses IMDB's non-commercial dataset to find the shortest
link between any two given actors, directors, composers, etc. It then
uses that information to retrieve information and images from TMDB's
API which is then finally used to create a unique visual representation
of any given connection.

The available scrypt is divided into five distinct sections: the data
retrieval and loading onto a PostgreSQL server, a breadth-first search
algorithm that makes database calls, an API module which makes requests
and downloads images, and data processing functions that produce the
visualization of the data. A final module test.py was created with pytest
to support a continuous test driven development cycle where each new
feature and associated functions work as intended.

This project was conceived as a learning means for the creator, and it
is, ultimately, an exercise in stringing together several technologies,
modules, skills, and programming, and computer science challenges.

The first challenge was to come up with the path finding algorithm.
My solution was to use the "stars" table for finding relationships along
with the python collections.deque for efficiently adding to the back of
the cue, and removing from the front. The most suitable, and largest
dataset, provided courtesy of IMDB had to be parsed, cleaned, and loaded
onto a,smaller database that exactly answered the needs of the project.
The could be considered fairly completed at that point but a recent want
to learn and use APIs led to the challenge of throughly reading the
documentation for the TMDB API and using that in conjunction with the
Pillow module for creating a unique visual representation of the path
found.



Notices:
Information courtesy of IMDb
(https://www.imdb.com).
Used with permission.

Documentation for the dataset can be found at: 
https://developer.imdb.com/non-commercial-datasets/

This product uses the TMDB API but is not endorsed or certified by TMDB.
https://developer.themoviedb.org/docs




2. Data Cleaning

Loading and using all the available data onto the database results in
a lengthy process which also leads to less than optimal results from
the rest of the program. However, no good compromise was found between
parsing data at insertion time and achieving the optimal data quality.
The script currently loads all of the data and then cleans it in a way
that was found to reasonable results in querying user name searches and
meaningfull connections between people; while still maintaining as much
range of information as possible.

At the time of writing, IMDB's data contains ten million entries in its
title.basics from which the table movies is based on. Only about
600,000 of those are classified as "movies" with the vast majority of
the 10 million being classified as tv-shows (see Query Example No. 1 below).
The database also contains adult movies and videos (329 thousand). For the
objectives of this project, parsing and removing some of the data is a
necessity in the movies table, but that also implies branching changes
in the stars, people, and ratings tables (see Table Schema below).

The five delete statements remove: title_types which are not in
('movie', 'short', 'tvSeries', 'tvMiniSeries'), movies who have between
zero and 20 votes, movies where the genre includes ('News', 'Talk-Show',
'Reality-TV', 'Adult'), movies that have a mature rating, and finally,
people who have no match for their IDs in the star table. Due to the
ON DELETE CASCADE statements at table creation, deleting from movies
also deletes from the stars table and ratings which implies that some
the people without a match in the stars table mostly worked is
those niches.


Query Example No. 1
SELECT title_type, count(*) FROM mock_movies GROUP BY title_type;
  title_type  |  count  
--------------+---------
 movie        |  665772
 short        |  966841
 tvEpisode    | 7942748
 tvMiniSeries |   51530
 tvMovie      |  143708
 tvPilot      |       1
 tvSeries     |  253717
 tvShort      |   10110
 tvSpecial    |   44775
 video        |  284254
 videoGame    |   36778


Table Schema
CREATE TABLE movies (
    id INTEGER PRIMARY KEY,
    title TEXT,
    year INTEGER
    title_type TEXT,
    is_adult INTEGER,
    runtime INTEGER,
    genres TEXT
);

CREATE TABLE people (
    id INTEGER PRIMARY KEY,
    name TEXT,
    birth INTEGER,
    death INTEGER,
    famous_movies TEXT
);

CREATE TABLE stars (
    person_id INTEGER REFERENCES people (id) ON DELETE CASCADE,
    movie_id INTEGER REFERENCES movies (id) ON DELETE CASCADE,
    category TEXT,
    UNIQUE (person_id, movie_id)
);

CREATE TABLE ratings (
    movie_id INTEGER REFERENCES movies (id) ON DELETE CASCADE,
    average NUMERIC,
    num_votes INTEGER
);




3. Requirements

This script requires that the following be installed within the Python
environment you are running this script in:

pillow (image manipulation and creation)
psycopg2 (PostgreSQL database CRUD  )
requests (TMDB API and IMDB dataset)
pytest (project tests)

A valid PostgreSQL database server connection is required
(Changeable in main.py, see tutorial)

Aditionally, your own private TMDB API key is necessary
An "apiauth.py" file with the following information:
headers = {"accept": "application/json", "Authorization": "Bearer YOUR_TOKEN_HERE"}
should work seamlessly




4. Tutorial - How to use main.py

Most of the functionality can be modifield and options can be toggled
within main.py and specifically under :
if __name__ == '__main__'

Every function has its own detailed documentation which can be accessed
with help(function_name). The setup outlined below should set-up the
program to run as intended while a few further comments will be provided below


if __name__ == '__main__':

    con = psycopg2.connect(database='mytest')
    cur = con.cursor()

    os.makedirs('./images', exist_ok=True)

    # Checks if all required tables exist
    load_imdb.check_database(con)


    # # Initial load
    # load_imdb.get_files()
    # load_imdb.make_movies(con)
    # load_imdb.make_ratings(con)
    # load_imdb.make_people(con)
    # load_imdb.make_stars(con)
    # load_imdb.create_indexes(con)
    # load_imdb.clean_data(con)


    # # Gets two IDs from user
    id_1 = get_input(1)
    id_2 = get_input(2)

    # Finds shortest path
    path = solve(id_1, id_2)

    # Gets info on each people pair
    info = get_complete_info(path, print_to_console=True)

    # Requests images to API
    info_images = get_images(info)

    # Creates a collage for each pair
    info_collage = create_collages(info_images)

    # Creates collage-strip
    merge_images(info_collage, show_image=True)

1. database='mytest'
Corresponds to a sample database and will need to be changed according
to the user's PostgreSQL set-up

2. Functions under "Initial load" should be commented out after all the
data has been loaded in.

3. In case user wants to simplify the game to the ariginal Six Degrees
of Kevin Bacon, id_2 can be set to the integer 102

4. User can comment out all the functions below get_complete_info() to
not make any API calls or produce the collages, simply printing to
console the resulting path between two people.




5. Future improvements

Optimizations could certaily be made on a few places. A few basic ones
are summarized below.

1. The textual search for a name within the database that matches the
user input could be improved due to the way the search is currently
conducted:
    a. Currently, if input contains two names, query assumes both come
    in that order
    b. If there are multiple spaces between two names, it will not
    find a match
    c. Query does not account for obvious search queries that do not
    match other common spellings (for example, Charlie Chaplin does
    not return 'Charles Chaplin')
    d. It currently ranks names that are associated with several
    'famous_movies' movies higher, but could be optimized for: closeness
    to input name, total movie quantity and their scores, other users
    queries (time-weighted), etc.

2. Currently very little information from the API request is actually
put to use

3. The solve(id1, id2) function is the most costly part of the script due to
requring multiple transactions to the database, a few potential
improvements are suggested are:
    a. Current implementation considers all nodes equally as likely
    and treats them with a first-come-first-served basis.
    Other solutions may involve joining tables which could be
    beneficial in some aspects.
    For example, table people could have a popularity metric based off
    of the number of other people associated with them.
    Another option would be to de-normalize stars table to have a
    "popularity" metric, which couldb be used to rank and sort the deque
    (although sorting could end up being costly there too)
    b. Each loop currently involves a unique query to the database,
    batch processing could be inplemented to cut down on database calls.
