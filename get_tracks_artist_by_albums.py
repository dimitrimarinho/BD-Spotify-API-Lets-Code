import api_functionality
import save_content_postgresql as scp
import pandas as pd 

scp.create_table('artist')
scp.create_table('album')
scp.create_table('track')

search_artist_name = input('Type the name of the artist that will be searched: ')
df_artists = api_functionality.search_artist(artist_name=search_artist_name)
df_artists = api_functionality.choose_kept_data(df_artists)
api_functionality.print_data_frame(df_artists)
scp.save_register(df_artists,'artist')

list_df_albums = []
for id_art in df_artists['id_artist']:
    df_albums = api_functionality.get_all_albums(artist_id=id_art)
    df_albums = api_functionality.choose_kept_data(df_albums)
    list_df_albums.append(df_albums)
df_albums_artist = pd.concat(list_df_albums, axis=0,ignore_index=True)
api_functionality.print_data_frame(df_albums_artist)
scp.save_register(df_albums_artist,'album')

list_df_tracks = []
for id in df_albums['id_album']:
    list_df_tracks.append(api_functionality.get_all_tracks_album(id))
df_tracks_artist = pd.concat(list_df_tracks, axis=0,ignore_index=True)
api_functionality.print_data_frame(df_tracks_artist)
scp.save_register(df_tracks_artist,'track')

print('Lista de todas as  músicas:')
df_all = df_tracks_artist.merge(df_albums,how='inner',left_on='id_album',right_on='id_album')\
.merge(df_artists,how='inner',left_on='id_artist',right_on='id_artist')[['Name_track','Name_album','Name_artist','duration_ms','release_date']]
api_functionality.print_data_frame(df_all)