import api_functionality
import save_content_postgresql

df_music = api_functionality.search_artist(artist_name='oficina g3')

api_functionality.print_data_frame(df_music)

save_content_postgresql.save_search_artist(df_music)