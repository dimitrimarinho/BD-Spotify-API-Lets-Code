import api_functionality
import pandas as pd 

df_artist = pd.DataFrame({'id_artist':['0gO5Vbklho8yrBrUdHhuLH'],'Name_artist':['Oficina G3']})
api_functionality.print_search_results(df_artist)

for id_art in df_artist['id_artist']:
    df_albums = api_functionality.get_all_albums(artist_id=id_art)
    api_functionality.print_search_results(df_albums)

    list_df_tracks = []
    for id in df_albums['id_album']:
        list_df_tracks.append(api_functionality.get_all_tracks_album(id))

    df_tracks_artist = pd.concat(list_df_tracks, axis=0,ignore_index=True)

    api_functionality.print_search_results(df_tracks_artist)

df_all = df_tracks_artist.merge(df_albums,how='inner',left_on='id_album',right_on='id_album')\
.merge(df_artist,how='inner',left_on='id_artist',right_on='id_artist')[['Name_track','Name_album','Name_artist','duration_ms','release_date']]
api_functionality.print_search_results(df_all)