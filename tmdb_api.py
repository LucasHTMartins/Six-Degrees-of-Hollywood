'''
Main module responsible for connecting to the TMDB API,
processing JSON response, and requesting posters and pictures
'''

import apiauth
import requests
import json


def get_poster(movie_id: int) -> str:
    '''Makes requests TMDB API and retrieves a poster for a given movie

    Parameters
    ----------
    movie_id : int
        The unique ID used within the PostgreSQL database

    Returns
    -------
    str
        Path to the saved poster
    str
        Empty string if status code != 200 or image is not found

    Side Effects
    ------------
    Downloads and saves the poster in the current working directory
    '''

    imdb_id = 'tt' + str(movie_id).zfill(7) # matches IMDB's internal ID

    url = f'https://api.themoviedb.org/3/find/{imdb_id}?external_source=imdb_id'

    response = requests.get(url, headers=apiauth.headers)
    if response.status_code != 200: return ''

    json_data = json.loads(response.text)

    try:
        poster_path = json_data['movie_results'][0]['poster_path']
    except IndexError:
        return ''

    poster_http = 'https://image.tmdb.org/t/p/original/' + poster_path
    file_name = f'{imdb_id}.jpg'

    response = requests.get(poster_http, headers=apiauth.headers)
    if response.status_code != 200: return ''

    with open(f'images/{imdb_id}.png', 'wb') as file:
        file.write(response.content)

    return f'images/{imdb_id}.png'


def get_artist(artist_id: int) -> str:
    '''Makes requests TMDB API and retrieves the profile picture for a given person

    Parameters
    ----------
    artist_id : int
        The unique ID used within the PostgreSQL database

    Returns
    -------
    str
        Path to the saved profile picture
    str
        Empty string if status code != 200 or image is not found

    Side Effects
    ------------
    Downloads and saves the profile picture in the current working directory
    '''

    imdb_id = 'nm' + str(artist_id).zfill(7) # matches IMDB's internal ID
    url = f'https://api.themoviedb.org/3/find/{imdb_id}?external_source=imdb_id'

    response = requests.get(url, headers=apiauth.headers)
    if response.status_code != 200: return ''

    json_data = json.loads(response.text)

    try:
        profile_path = json_data['person_results'][0]['profile_path']
    except IndexError:
        return ''

    profile_http = 'https://image.tmdb.org/t/p/original/' + profile_path
    file_name = f'{imdb_id}.jpg'

    response = requests.get(profile_http, headers=apiauth.headers)
    if response.status_code != 200: return ''

    with open(f'images/{file_name}.png', 'wb') as file:
        file.write(response.content)

    return f'images/{file_name}.png'


if __name__ == '__main__':
    pass

