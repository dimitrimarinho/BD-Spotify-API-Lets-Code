import requests
import authorization
import pandas as pd
import numpy as np
from tabulate import tabulate
import psycopg2

def search_track(track_name,artist_name):
    # base URL of all Spotify API endpoints
    BASE_URL = 'https://api.spotify.com/v1/'
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

def print_search_results(df_searched_result):  
    print(tabulate(df_searched_result, headers='keys', tablefmt='psql'))

def choose_kept_date(dataframe_searched_music):
    keep_searched  = [int(id) for id in input('Which tracks will be saved in database? ').split(',')]
    df_kept = dataframe_searched_music.iloc[keep_searched].reset_index(drop=True)
    return df_kept

def addapt_numpy_int64(numpy_int64):
    return psycopg2.extensions.AsIs(numpy_int64)
psycopg2.extensions.register_adapter(np.int64,addapt_numpy_int64)
    
df_music = search_track(track_name='espelhos mágicos',artist_name='oficina g3')
print_search_results(df_music)

df_kept_musics = choose_kept_date(df_music)

print_search_results(df_kept_musics)

# Adding to database
secrets = authorization.db_secrets()

conn = psycopg2.connect(host=secrets['host'],
        port=secrets['port'],
        database=secrets['database'], 
        user=secrets['user'], 
        password=secrets['pass'], 
        connect_timeout=3)

cur = conn.cursor()

sql = 'CREATE TABLE IF NOT EXISTS track_tb (id SERIAL PRIMARY KEY, name VARCHAR,album VARCHAR,artists VARCHAR,duration INT);'
cur.execute(sql)
conn.commit()


for idx in df_kept_musics.index: 
    sql = 'INSERT INTO track_tb(name,album,duration,artists) VALUES ( %s, %s, %s,%s)'
    cur.execute(sql,tuple(df_kept_musics.iloc[idx].to_list()))
    if idx % 50 == 0 : conn.commit()
conn.commit()
cur.close()
