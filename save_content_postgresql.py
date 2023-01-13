import psycopg2
import authorization
import numpy as np
import psycopg2

def addapt_numpy_int64(numpy_int64):
    return psycopg2.extensions.AsIs(numpy_int64)
psycopg2.extensions.register_adapter(np.int64,addapt_numpy_int64)

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
        'artist':'create table IF NOT EXISTS artist (id serial primary key, id_artist varchar(100) UNIQUE, Name_artist varchar(100), genres varchar(100), popularity_artist integer)',
        'album':'create table IF NOT EXISTS album (id serial primary key, id_album varchar(100) UNIQUE, Name_album varchar(100), release_date varchar(100), popularity_album integer, id_artist varchar(100))',
        'track':'create table IF NOT EXISTS track (id serial primary key, id_track varchar(100) UNIQUE, Name_track varchar(100), duration_ms int, popularity_track integer, id_album varchar(100))'
    }
    con = create_connection_spotify_db()
    cur = con.cursor()
    sql = map_sql_command[type_table]
    cur.execute(sql)
    con.commit()
    con.close()

def new_create_table(type_table):
    map_sql_command = {
        'artist':'create table IF NOT EXISTS artist (id serial primary key, id_artist varchar(100) UNIQUE, Name_artist varchar(100), genres varchar(100), popularity_artist integer)',
        'album':'create table IF NOT EXISTS album (id serial primary key, id_album varchar(100) UNIQUE, Name_album varchar(100), release_date varchar(100), popularity_album integer, id_artist integer references artist(id) on delete cascade)',
        'track':'create table IF NOT EXISTS track (id serial primary key, id_track varchar(100) UNIQUE, Name_track varchar(100), duration_ms int, popularity_track integer, id_album integer references album(id) on delete cascade)'
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
        'artist':"INSERT INTO artist (id_artist, Name_artist, genres, popularity_artist) VALUES (%s, %s, %s, %s) ON CONFLICT (id_artist) DO NOTHING returning artist;",
        'album':"INSERT INTO album (id_album, Name_album, release_date, popularity_album, id_artist) VALUES (%s, %s, %s, %s, %s) ON CONFLICT (id_album) DO NOTHING returning album;",
        'track':"INSERT INTO track (id_track, Name_track, duration_ms, popularity_track, id_album) VALUES (%s, %s, %s, %s, %s) ON CONFLICT (id_track) DO NOTHING returning track;", 
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

def new_save_artist_register(df_artist):
    con = create_connection_spotify_db()
    cur = con.cursor()
    args_str = ','.join(cur.mogrify("(%s,%s,%s,%s)", row).decode('utf-8') for _,row in df_artist.iterrows())
    query = "INSERT INTO artist (id_artist, Name_artist, genres, popularity_artist) VALUES "+args_str+" ON CONFLICT (id_artist) DO NOTHING returning artist;"
    cur.execute(query)
    con.commit()
    con.close()

def new_save_album_register(df_album):
    con = create_connection_spotify_db()
    cur = con.cursor()
    query = "(SELECT id,id_artist from artist WHERE id_artist IN %s)"
    cur.execute(query,(tuple(df_album['id_artist']),))
    album_ids = cur.fetchall()
    df = df_album
    mapping = {id[1]:str(id[0]) for id in album_ids}
    df['id_artist'] = df['id_artist'].map(mapping).astype(np.int64)
    args_str = ','.join(cur.mogrify("(%s,%s,%s,%s,%s)", row).decode('utf-8') for _,row in df.iterrows())
    query = "INSERT INTO album (id_album, Name_album, release_date, popularity_album, id_artist) VALUES "+args_str+" ON CONFLICT (id_album) DO NOTHING returning album;"
    cur.execute(query)
    con.commit()
    con.close()

def new_save_track_register(df_track):
    con = create_connection_spotify_db()
    cur = con.cursor()
    query = "(SELECT id,id_album from album WHERE id_album IN %s)"
    cur.execute(query,(tuple(df_track['id_album']),))
    album_ids = cur.fetchall()
    df = df_track
    mapping = {id[1]:str(id[0]) for id in album_ids}
    df['id_album'] = df['id_album'].map(mapping).astype(np.int64)
    args_str = ','.join(cur.mogrify("(%s,%s,%s,%s,%s)", row).decode('utf-8') for _,row in df.iterrows())
    query = "INSERT INTO track (id_track, Name_track, duration_ms, popularity_track, id_album) VALUES "+args_str+" ON CONFLICT (id_track) DO NOTHING returning track;" 
    cur.execute(query)
    con.commit()
    con.close()

def new_save_register(df_result,type_table):
    map_save_function = {
        'artist':new_save_artist_register,
        'album':new_save_album_register,
        'track':new_save_track_register
    }
    save_function = map_save_function[type_table]
    save_function(df_result)