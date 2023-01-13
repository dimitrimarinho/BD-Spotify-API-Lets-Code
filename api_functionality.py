import requests
import authorization
import pandas as pd
from tabulate import tabulate

# base URL of all Spotify API endpoints
BASE_URL = 'https://api.spotify.com/v1/'

def request_search(**kwargs):
    query = ''
    if 'q' in kwargs.keys:
        query += f"q={kwargs['q']}&"
    if 'type' in kwargs.keys:
        query += f"type={kwargs['type']}&"
    if 'include_external' in kwargs.keys:
        query += f"include_external={kwargs['include_external']}"
    if 'limit' in kwargs.keys:
        query += f"limit={kwargs['limit']}&"
    if 'market' in kwargs.keys:
        query += f"market={kwargs['market']}&"
    if 'offset' in kwargs.keys:
        query += f"offset={kwargs['offset']}&"
    query = query[:-2]
    search_result = requests.get(BASE_URL + 'search?'+query, headers=authorization.get_headers())
    search_result_json = search_result.json()
    return search_result_json

def search_track(track_name,artist_name):
    # actual GET request with proper header
    search_result = requests.get(BASE_URL + 'search?' + f'q=track:{track_name}%20artist:{artist_name}&type=track&limit=5', headers=authorization.get_headers())
    search_result = search_result.json()
    data_searched = {'Name':[],'Album':[],'Duration (ms)':[],'Artists':[]}
    for item in search_result['tracks']['items']:
        data_searched['Name'].append(item["name"])
        data_searched['Album'].append(item["album"]["name"])
        data_searched['Duration (ms)'].append(item["duration_ms"])
        artists = []
        for artist in item['artists']:
            artists.append(artist['name'])
        data_searched['Artists'].append(','.join(artists))
    df_searched_result = pd.DataFrame(data_searched)
    return df_searched_result

def search_artist(artist_name):
    # actual GET request with proper header
    search_result = requests.get(BASE_URL + 'search?' + f'q=%20artist:{artist_name}&type=artist&limit=5', headers=authorization.get_headers())
    search_result = search_result.json()

    data_searched = {'id_artist':[],'Name_artist':[],'genres':[],'popularity_artist':[]}
    for item in search_result['artists']['items']:
        data_searched['Name_artist'].append(item['name'])
        data_searched['id_artist'].append(item['id'])
        data_searched['genres'].append(','.join(item['genres']))
        data_searched['popularity_artist'].append(item['popularity'])
        
    df_searched_result = pd.DataFrame(data_searched)
    return df_searched_result

def get_all_albums(artist_id):
    search_result = requests.get(BASE_URL + 'artists/' + f'{artist_id}'+ '/albums?limit=50&include_groups=album', headers=authorization.get_headers())
    search_result = search_result.json()

    album_ids=[]
    for item in search_result['items']:
        album_ids.append(item['id'])

    return get_several_albums(album_ids)

def get_all_tracks_album(album_id):
    search_result = requests.get(BASE_URL + 'albums/' + f'{album_id}'+ '/tracks?limit=50', headers=authorization.get_headers())
    search_result = search_result.json()

    id_tracks = []
    for item in search_result['items']:
        id_tracks.append(item['id'])
    
    return get_several_tracks(id_tracks)

def get_artist(artist_id):
    search_result = requests.get(BASE_URL + 'artists/' + f'{artist_id}'+ '?limit=50', headers=authorization.get_headers())
    search_result = search_result.json()

    data_searched = {
        'id_artist':[artist_id],
        'Name_artist':[search_result['name']],
        'genres':[search_result['genres']],
        'popularity_artist':[search_result['popularity']]}

    df_get_result = pd.DataFrame(data_searched)
    return df_get_result

def split_list (list, batch):
   return [list[i:i+batch] for i in range(0, len(list), batch)]

def get_several_artists(id_artists):
    splitted_list_id = split_list(id_artists,50)
    data_searched = {'id_artist':[],'Name_artist':[],'genres':[],'popularity_artist':[]}
    for list_ids in splitted_list_id:
        search_result = requests.get(BASE_URL + 'artists?' + f'ids={",".join(list_ids)}', headers=authorization.get_headers())
        search_result = search_result.json()
        for item in search_result['artists']:
            data_searched['Name_artist'].append(item['name'])
            data_searched['id_artist'].append(item['id'])
            data_searched['genres'].append(','.join(item['genres']))
            data_searched['popularity_artist'].append(item['popularity'])
    df_get_result = pd.DataFrame(data_searched)
    return df_get_result

def get_album(album_id):
    search_result = requests.get(BASE_URL + 'albums/' + f'{album_id}'+ '?limit=50', headers=authorization.get_headers())
    search_result = search_result.json()

    data_searched = {
        'id_album':[search_result['id']],
        'Name_album':[search_result['name']],
        'release_date':[search_result['release_date']],
        'popularity_album':[search_result['popularity']],
        'id_artist':[album_id]}

    df_get_result = pd.DataFrame(data_searched)
    return df_get_result

def get_several_albums(id_albums):
    splitted_list_id = split_list(id_albums,20)
    data_searched = {'id_album':[],'Name_album':[],'release_date':[],'popularity_album':[],'id_artist':[]}
    for list_ids in splitted_list_id:
        search_result = requests.get(BASE_URL + 'albums?' + f'ids={",".join(list_ids)}', headers=authorization.get_headers())
        search_result = search_result.json()
        for item in search_result['albums']:
            data_searched['Name_album'].append(item['name'])
            data_searched['id_album'].append(item['id'])
            data_searched['release_date'].append(item['release_date'])
            data_searched['popularity_album'].append(item['popularity'])
            data_searched['id_artist'].append(item['artists'][0]['id'])
    df_get_result = pd.DataFrame(data_searched)
    return df_get_result

def get_several_tracks(id_tracks):
    splitted_list_id = split_list(id_tracks,50)
    data_searched = {'id_track':[],'Name_track':[],'duration_ms':[],'popularity_track':[], 'id_album':[]}
    for list_ids in splitted_list_id:
        search_result = requests.get(BASE_URL + 'tracks?' + f'ids={",".join(list_ids)}', headers=authorization.get_headers())
        search_result = search_result.json()
        for item in search_result['tracks']:
            data_searched['id_track'].append(item['id'])
            data_searched['Name_track'].append(item['name'])
            data_searched['duration_ms'].append(item['duration_ms'])
            data_searched['popularity_track'].append(item['popularity'])
            data_searched['id_album'].append(item['album']['id'])
    df_get_result = pd.DataFrame(data_searched)
    return df_get_result

def print_data_frame(df_searched_result):  
    print(tabulate(df_searched_result, headers='keys', tablefmt='psql'))

def choose_kept_data(dataframe):
    print_data_frame(dataframe)
    keep_searched  = [int(id) for id in input('Which data will be saved in the database? ').split(',')]
    print()
    df_kept = dataframe.iloc[keep_searched].reset_index(drop=True)
    return df_kept

def search_most_popular_tracks_by_year(number,year):
    data_searched_artist = set()
    data_searched_album = set()
    data_searched_track = {'id_track':[],'Name_track':[],'duration_ms':[],'popularity_track':[],'id_album':[]}
    
    request_offset = 0
    request_limit = 50 if number-50 >= 0 else number
    
    while number != 0:
        endpoint = f"https://api.spotify.com/v1/search?query=year%3A{year}&type=track&offset={request_offset}&limit={request_limit}"
        search_result = requests.get(endpoint, headers=authorization.get_headers())
        search_result = search_result.json()
        
        for item in search_result['tracks']['items']:
            data_searched_track['Name_track'].append(item['name'])
            data_searched_track['id_track'].append(item['id'])
            data_searched_track['duration_ms'].append(item['duration_ms'])
            data_searched_track['popularity_track'].append(item['popularity'])
            id_artist = item['artists'][0]['id']
            id_album = item['album']['id']
            data_searched_track['id_album'].append(id_album)
            data_searched_artist.add(id_artist)
            data_searched_album.add(id_album)  

        number -= search_result['tracks']['limit']
        request_offset += search_result['tracks']['limit'] 
        request_limit = 50 if number-50 >= 0 else number
    
    data_searched_artist = list(data_searched_artist)
    data_searched_album = list(data_searched_album)
    df_searched_artist = get_several_artists(data_searched_artist)
    df_searched_album = get_several_albums(data_searched_album)
    df_searched_track = pd.DataFrame(data_searched_track)
    
    return df_searched_artist,df_searched_album,df_searched_track

def get_tracks_artist_by_albums(search_artist_name):
    df_artists = search_artist(artist_name=search_artist_name)
    df_artists = choose_kept_data(df_artists)
    print_data_frame(df_artists)

    list_df_albums = []
    for id_art in df_artists['id_artist']:
        df_albums = get_all_albums(artist_id=id_art)
        df_albums = choose_kept_data(df_albums)
        list_df_albums.append(df_albums)
    df_albums_artist = pd.concat(list_df_albums, axis=0,ignore_index=True)
    print_data_frame(df_albums_artist)

    list_df_tracks = []
    for id in df_albums['id_album']:
        list_df_tracks.append(get_all_tracks_album(id))
    df_tracks_artist = pd.concat(list_df_tracks, axis=0,ignore_index=True)
    print_data_frame(df_tracks_artist)

    return df_artists,df_albums_artist,df_tracks_artist

