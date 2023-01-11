import api_functionality

df_music = api_functionality.search_artist(artist_name='oficina g3')
api_functionality.print_search_results(df_music)