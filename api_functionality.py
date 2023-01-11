import requests
import authorization
import pandas as pd
import numpy as np
from tabulate import tabulate
import psycopg2

# base URL of all Spotify API endpoints
BASE_URL = 'https://api.spotify.com/v1/'

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

    data_searched = {'id_artist':[],'Name_artist':[],'genres':[],'popularity':[]}
    for item in search_result['artists']['items']:
        data_searched['Name_artist'].append(item['name'])
        data_searched['id_artist'].append(item['id'])
        data_searched['genres'].append(','.join(item['genres']))
        data_searched['popularity'].append(item['popularity'])
        
    df_searched_result = pd.DataFrame(data_searched)
    return df_searched_result

def get_all_albums(artist_id):
    search_result = requests.get(BASE_URL + 'artists/' + f'{artist_id}'+ '/albums?limit=50&include_groups=album', headers=authorization.get_headers())
    search_result = search_result.json()

    data_searched = {'id_album':[],'Name_album':[],'release_date':[],'id_artist':[]}
    for item in search_result['items']:
        data_searched['Name_album'].append(item['name'])
        data_searched['id_album'].append(item['id'])
        data_searched['release_date'].append(item['release_date'])
        data_searched['id_artist'].append(artist_id)
    
    df_searched_result = pd.DataFrame(data_searched)
    return df_searched_result

def get_all_tracks_album(album_id):
    search_result = requests.get(BASE_URL + 'albums/' + f'{album_id}'+ '/tracks?limit=50', headers=authorization.get_headers())
    search_result = search_result.json()

    data_searched = {'id_track':[],'Name_track':[],'duration_ms':[],'id_album':[]}
    for item in search_result['items']:
        data_searched['Name_track'].append(item['name'])
        data_searched['id_track'].append(item['id'])
        data_searched['duration_ms'].append(item['duration_ms'])
        data_searched['id_album'].append(album_id)
    
    df_searched_result = pd.DataFrame(data_searched)
    return df_searched_result

def print_data_frame(df_searched_result):  
    print(tabulate(df_searched_result, headers='keys', tablefmt='psql'))

def choose_kept_data(dataframe):
    print_data_frame(dataframe)
    keep_searched  = [int(id) for id in input('Which data will be saved in the database? ').split(',')]
    print()
    df_kept = dataframe.iloc[keep_searched].reset_index(drop=True)
    return df_kept

def addapt_numpy_int64(numpy_int64):
    return psycopg2.extensions.AsIs(numpy_int64)
psycopg2.extensions.register_adapter(np.int64,addapt_numpy_int64)
    


