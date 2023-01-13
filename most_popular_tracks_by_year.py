import api_functionality
import save_content_postgresql as scp
import pandas as pd 

scp.create_table('artist')
scp.create_table('album')
scp.create_table('track')

df_artist,df_album,df_track = api_functionality.search_most_popular_tracks_by_year(102,2022)
api_functionality.print_data_frame(df_artist)
api_functionality.print_data_frame(df_album)
api_functionality.print_data_frame(df_track)

scp.save_register(df_artist,'artist')
scp.save_register(df_album,'album')
scp.save_register(df_track,'track')