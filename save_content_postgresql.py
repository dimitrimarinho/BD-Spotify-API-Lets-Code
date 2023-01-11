import psycopg2
import pandas as pd

def create_table_spotify():
    con = psycopg2.connect(host='localhost', database='spotify-api',
    user='postgres', password='root')
    cur = con.cursor()
    sql = 'create table spotify (id serial primary key, id_artist varchar(100), Name_artist varchar(100), genres varchar(100), popularity integer)'
    cur.execute(sql)
    con.commit()
    cur.execute('select * from spotify')
    recset = cur.fetchall()
    for rec in recset:
        print(rec)
    con.close()

def save_search_artist(df_searched_result):
    con = psycopg2.connect(host='localhost', database='spotify-api',
    user='postgres', password='root')    
    cur = con.cursor()
    for index, row in df_searched_result.iterrows():
        cur.execute("INSERT INTO spotify (id_artist, Name_artist, genres, popularity) VALUES (%s, %s, %s, %s) returning spotify;", (row['id_artist'], row['Name_artist'],row['genres'], row['popularity']))
        con.commit()
    recset = cur.fetchall()
    for rec in recset:
        print('\n\nÚltimo registro salvo no Banco de dados: ', rec)
    con.close()
