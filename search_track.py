import authorization
import api_functionality
import psycopg2

df_music = api_functionality.search_track(track_name='espelhos mágicos',artist_name='oficina g3')
api_functionality.print_search_results(df_music)

df_kept_musics = api_functionality.choose_kept_date(df_music)

api_functionality.print_search_results(df_kept_musics)

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
