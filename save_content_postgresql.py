import psycopg2
import authorization

def create_connection_spotify_db():
    secrets = authorization.db_secrets()

    conn = psycopg2.connect(host=secrets['host'],
        port=secrets['port'],
        database=secrets['database'], 
        user=secrets['user'], 
        password=secrets['pass'], 
        connect_timeout=3)    
    return conn

def create_table_spotify():
    con = create_connection_spotify_db()
    cur = con.cursor()
    sql = 'create table IF NOT EXISTS spotify (id serial primary key, id_artist varchar(100) UNIQUE, Name_artist varchar(100), genres varchar(100), popularity integer)'
    cur.execute(sql)
    con.commit()
    cur.execute('select * from spotify')
    recset = cur.fetchall()
    for rec in recset:
        print(rec)
    con.close()

def create_table(type_table):
    map_sql_command = {
        'artist':'create table IF NOT EXISTS artist (id serial primary key, id_artist varchar(100) UNIQUE, Name_artist varchar(100), genres varchar(100), popularity integer)',
        'album':'create table IF NOT EXISTS album (id serial primary key, id_album varchar(100) UNIQUE, Name_album varchar(100), release_date varchar(100), id_artist varchar(100))',
        'track':'create table IF NOT EXISTS track (id serial primary key, id_track varchar(100) UNIQUE, Name_track varchar(100), duration_ms int, id_album varchar(100))'
    }
    con = create_connection_spotify_db()
    cur = con.cursor()
    sql = map_sql_command[type_table]
    cur.execute(sql)
    con.commit()
    con.close()

def save_search_artist(df_searched_result):
    con = create_connection_spotify_db()
    cur = con.cursor()
    for index, row in df_searched_result.iterrows():
        cur.execute("INSERT INTO spotify (id_artist, Name_artist, genres, popularity) VALUES (%s, %s, %s, %s) ON CONFLICT (id_artist) DO NOTHING returning spotify;", (row['id_artist'], row['Name_artist'],row['genres'], row['popularity']))
        con.commit()
    recset = cur.fetchall()
    for rec in recset:
        print('\n\nÚltimo registro salvo no Banco de dados: ', rec)
    con.close()

def save_register(df_result,type_table):
    con = create_connection_spotify_db()
    cur = con.cursor()
    map_sql_command = {
        'artist':"INSERT INTO artist (id_artist, Name_artist, genres, popularity) VALUES (%s, %s, %s, %s) ON CONFLICT (id_artist) DO NOTHING returning artist;",
        'album':"INSERT INTO album (id_album, Name_album, release_date, id_artist) VALUES (%s, %s, %s, %s) ON CONFLICT (id_album) DO NOTHING returning album;",
        'track':"INSERT INTO track (id_track, Name_track, duration_ms, id_album) VALUES (%s, %s, %s, %s) ON CONFLICT (id_track) DO NOTHING returning track;", 
    }
    sql = map_sql_command[type_table]
    for index, row in df_result.iterrows():
        cur.execute(sql,tuple(row))
        if index % 50 == 0: con.commit()
    con.commit()
    recset = cur.fetchall()
    for rec in recset:
        print('\n\nÚltimo registro salvo no Banco de dados: ', rec)
    con.close()