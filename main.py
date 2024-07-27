'''
Central script for: Six Degrees of Kevin Bacon

Contains central functions and program logic,
functions and usage parameters can be changed at __name__ == '__main__'
'''

import psycopg2
import csv
import collections
import sys
from PIL import Image, ImageDraw, ImageFont
import os
import time

import tmdb_api
import load_imdb


def get_input(num: int) -> int:
    '''Prompts user for input

    Loops over check_for_id() and check_name() until it finds a match
    Check for ID is called first since it is transactionly cheaper
    Prints final choice

    Parameters
    ----------
    num : int
        Used to prompt user for one of two inputs

    Returns
    -------
    int
        the ID to a valid person within the database
    '''

    while True:
        u_input = input(f'\nName (or ID) for person No.{num}: ')

        if u_input == '\\q':
            sys.exit()

        person_info = check_for_id(u_input)
        if person_info: break

        person_info = check_name(u_input)
        if person_info: break

    if person_info[3] != None:
        string_titles = get_most_famous(person_info[3])
    else: string_titles = None

    print(f'\nYou chose \nName: {person_info[0]}')
    print(f'ID: {person_info[2]}')
    print(f'DOB: {person_info[1]}') if person_info[1] else None
    print(f'Known for: {string_titles}\n') if string_titles else None

    time.sleep(0.5)

    return person_info[2]


def check_for_id(input: str) -> list:
    '''Checks if user input is a valid ID

    Parameters
    ----------
    input : str
        User input from get_input()

    Returns
    -------
    None
        if user input is not an integer or does not match an ID
    list
        With a person's name, birth, ID, and string of famous movies
    '''

    try:
        id = int(input)

        search = '''SELECT name, birth, id, famous_movies
        FROM people WHERE id = %s;
        '''

        cur.execute(search, (id,))
        result = cur.fetchall()
        assert len(result) == 1

    except (ValueError, AssertionError):
        return None

    person = result[0]
    return person


def check_name(input_name: str) -> list:
    '''Checks database for user input as a person's name

    Acts differently depending if query finds zero, one, or multiple results
    When multiple possible people are found, people that do not have famous
    movies (according to imdb's data) are removed, this filter could be dropped

    Parameters
    ----------
    input : str
        User input from get_input()

    Returns
    -------
    None
        if user input does not match a name or does not match an ID
    list
        With a person's name, birth, ID, and string of famous movies
    '''

    input_name = input_name.strip()
    person_1_search = r'(?:^|\s)' + input_name + r'(?:$|\s)'

    search = '''SELECT name, birth, id, famous_movies
        FROM people
        WHERE name ~* %s
        ORDER BY LENGTH(famous_movies) ASC;
        '''

    cur.execute(search, (person_1_search,))
    result = cur.fetchall()

    if len(result) == 0:
        print(f'\n{input_name}\nDoes not exist in the database, please try again.')
        return None

    if len(result) == 1:
        return result[0]

    if len(result) > 1:
        print('\nYour search returned multiple people, select one of the following and type the complete name OR ID:\n')

        for person in result:

            famous_list = get_most_famous(person[3])
            if not famous_list: continue

            print(f'Name {person[0]}')
            print(f'ID: {person[2]}')
            print(f'DOB: {person[1]}') if person[1] else None
            print(f'Known for: {famous_list}\n') if famous_list else None
            print()

        print('Your search returned multiple people, select one of the following and type the complete name OR ID:')
        print('Tip: the most likely results are pushed towards the bottom of the list')

        return None


def get_most_famous(string_titles: str) -> str:
    '''Returns a string with a person's notable titles

    Parameters
    ----------
    string_titles : str
        Comma-separated ID's for titles in IMDB's format

    Returns
    -------
    str
        a readable list of movies titles associated with a person
    '''
    if string_titles == '\\N': return False
    if string_titles == None: return False
    items = string_titles.split(',')
    result = [int(m_id[2:]) for m_id in items]
    cur.execute('SELECT title FROM movies WHERE id = ANY(%s);', (result, ))
    list_titles = [title[0] for title in cur.fetchall()]
    string_titles = ', '.join(list_titles) if list_titles else ''

    return string_titles


def find_contacts(i_id: int) -> list:
    '''Finds all other people associated with a given person through common movies

    Parameters
    ----------
    i_id : int
        The ID of a person (current node in solve())

    Returns
    -------
    list
        List of all other person's ID's associated with the given ID
    '''
    sql = '''
        SELECT DISTINCT s2.person_id
        FROM stars s1
        JOIN stars s2 ON s1.movie_id = s2.movie_id
        WHERE s1.person_id = %s AND s2.person_id != %s
    '''
    cur.execute(sql, (i_id, i_id))
    results = cur.fetchall()
    contacts = [result[0] for result in results]
    return contacts


def solve(start_node: int, target: int, max_nodes=1000000) -> list:
    '''Breadth-first algorithm to find the shortest path between two nodes

    Uses a Deque data type to increase the speed of the cue management

    Parameters
    ----------
    start_node : int
        Person No. 1's ID as given by the user
    target : int
        Person No. 2's ID as given by the user
    max_nodes : int
        Optional, limits the number of nodes visited by the algorithm
        while looking for a path

    Returns
    -------
    None
        If no path is found whithin the stipulated number of nodes
    list
        Path from node to node (people IDs) which are connected
        through movies
    '''

    queue = collections.deque([(start_node, [])])
    nodes_visited = 0
    path = []
    visited_ids = set()

    print('Looking for a connection...\n')

    while queue:
        nodes_visited += 1
        if nodes_visited > max_nodes:
            sys.exit("Program terminated: No connection found...")

        current_node, current_path = queue.popleft()

        if current_node in visited_ids:
            continue

        visited_ids.add(current_node)
        contacts = find_contacts(current_node)

        if target in contacts:
            return [target, current_node] + list(current_path)

        for contact in contacts:
            if contact not in visited_ids:
                queue.append((contact, [current_node] + current_path))


def get_complete_info(path: list, print_to_console=True) -> list:
    '''Processes the path found by the search agorithm and retrives
    all available database information regarding each pair of IDs.
    Number of dictionaries = (len(path) -1)

    Also creates a more natural sentence for each pair

    Each dictionary contains the following keys:
        'person_1_id', 'person_1_name', 'person_1_role',
        'movie_id', 'movie_title', 'movie_year', movie_rating
        'person_2_id', 'person_2_name', 'person_2_role',
        'sentence'

    Parameters
    ----------
    path : list
        List of people IDs
    print_to_console : bool
        Sets whether produced sentences get printed to console

    Returns
    -------
    list
        List of dictionaries
    '''

    info = []
    sent_maker = {
        'self': 'were themselves',
        'writer': 'was a writer',
        'editor': 'was an editor',
        'composer': 'was a composer',
        'production_designer': 'was a production designer',
        'cinematographer': 'was a cinematographer',
        'director': 'was a director',
        'archive_footage': 'was present in archive footage',
        'actress': 'was an actress',
        'producer': 'was a producer',
        'archive_sound': 'was in archive sound',
        'actor': 'was an actor',
    }

    query = '''SELECT p1.id, p1.name, s1.category,
        movies.id, movies.title, movies.year, ratings.average,
        p2.id, p2.name, s2.category
        FROM stars s1
        JOIN stars s2 ON s1.movie_id = s2.movie_id
        JOIN people p1 ON p1.id = s1.person_id
        JOIN people p2 ON p2.id = s2.person_id
        JOIN movies ON movies.id = s1.movie_id
        JOIN ratings ON ratings.movie_id = movies.id
        WHERE s1.person_id = %s
        AND s2.person_id = %s
        ORDER BY ratings.num_votes DESC
    ;'''

    for num in range(len(path) - 1):
        duo = {}

        cur.execute(query, (path[num], path[num + 1]))
        result = cur.fetchone()

        duo['person_1_id'] = result[0]
        duo['person_1_name'] = result[1]
        duo['person_1_role'] = result[2]

        duo['movie_id'] = result[3]
        duo['movie_title'] = result[4]
        duo['movie_year'] = result[5]
        duo['movie_rating'] = result[6]

        duo['person_2_id'] = result[7]
        duo['person_2_name'] = result[8]
        duo['person_2_role'] = result[9]

        sentence = f"{duo['person_1_name']} {sent_maker[duo['person_1_role']]}" \
              f" in {duo['movie_title']} {duo['movie_year']} where " \
              f"{duo['person_2_name']} {sent_maker[duo['person_2_role']]}."

        duo['sentence'] = sentence

        if print_to_console:
            print(sentence)
            time.sleep(0.5)

        info.append(duo)

    return info


def get_images(info: list) -> list:
    '''Connects to API module to request images

    The list of dictionaries containing all of the information now also
    gains a key 'paths' with a tuple of paths or empty string

    Parameters
    ----------
    info : list
        The list of dictionaries produced by get_complete_info()

    Returns
    -------
    list
        The list of dictionaries from the parameters

    Side Effects
    -------
    Makes up to 6 API calls and downloads up to 3 files
    '''

    for duo in info:

        p1 = tmdb_api.get_artist(duo['person_1_id'])
        p2 = tmdb_api.get_artist(duo['person_2_id'])
        poster = tmdb_api.get_poster(duo['movie_id'])

        if p1 == '':
            p1 = 'images/not_found.png'
        if p2 == '':
            p2 = 'images/not_found.png'
        if poster == '':
            poster = 'images/not_found.png'

        duo['paths'] = (p1, poster, p2)

    return info


def create_collages(info: list, base='images/base.png') -> list:
    '''Creates a collage for each people pair

    The list of dictionaries gains a key 'collage' which corresponds
    to the path to the image created

    Parameters
    ----------
    info
        List of dictionaries
    base : str
        Optional, uses the image specified as base for the illustrative creation
        Image must be .png and 1920x1080

    Returns
    -------
    list
        The list of dictionaries produced by get_images()

    Side Effects
    -------
    Creates an saves collages in the current directory.
    '''
    for duo in info:
        size = (440, 660)

        font_path = 'Pillow/Tests/fonts/FreeMono.ttf'
        font_size = 46

        img1_path, poster_path, img2_path = duo['paths']
        res_string = duo['sentence']

        p1 = Image.open(img1_path)
        p1.thumbnail(size)

        poster = Image.open(poster_path)
        poster.thumbnail(size)

        p2 = Image.open(img2_path)
        p2.thumbnail(size)

        box_p1 = (100, 60, 100 + p1.width, 60 + p1.height)
        box_poster = (740, 60, 1180, 60 + poster.height)
        box_p2 = (1820 - p2.width, 60, 1820, 60 + p2.height)

        img = Image.open(base)
        img.paste(p1, box_p1)
        img.paste(poster, box_poster)
        img.paste(p2, box_p2)

        draw = ImageDraw.Draw(img)
        font = ImageFont.truetype(font_path, font_size)
        text_w = draw.textsize(res_string, font=font)[0]
        img_w = img.size[0]

        while text_w > 1850:
            font_size -= 2
            font = ImageFont.truetype(font_path, font_size)
            text_w = draw.textsize(res_string, font=font)[0]

        text_x = (img_w - text_w) // 2
        draw.text((text_x, 910), res_string, font=font, fill=(0, 0, 0, 255))

        path = f"images/collage_for_{duo['movie_id']}.png"
        img.save(path)
        duo['collage'] = path

        img.close()
        p1.close()
        poster.close()
        p2.close()

    return info


def merge_images(info:list, result_path='images/image_strip.png', show_image=True):
    '''Collates sequence of images into a larger image-strip

    Parameters
    ----------
    info : list
        List of dictionaries
    result_path : str
        Optional, saves the image to the path specified
    show_image : bool
        Optional, opens the image when True

    Returns
    -------
    None

    Side Effects
    -------
    Downloads and saves the image-strip in the current specified directory.
    '''

    count = 0
    img = Image.open(info[0]['collage'])

    for duo in info:
        count += 1

        if count == len(info):
            img.save(result_path)
            if show_image: img.show()
            sys.exit()

        img2 = Image.open(info[count]['collage'])

        w = img.size[0] + img2.size[0]
        h = max(img.size[1], img2.size[1])

        sum_img = Image.new('RGBA', (w, h))
        sum_img.paste(img)
        sum_img.paste(img2, (img.size[0], 0))

        img = sum_img



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



'''
TO DO

Implement tests with pytest

'''
