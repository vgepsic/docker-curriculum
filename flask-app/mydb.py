import sqlite3
from sqlite3 import Error
import os

def db_execute(conn, sqlite_cmd):
    try:
        c = conn.cursor()
        c.execute(sqlite_cmd)
        return c
    except Error as e:
        print(e)

def create_connection(db_file):
    """ create a database connection to a SQLite database """
    conn = None
    try:
        conn = sqlite3.connect(db_file)
    except Error as e:
        print(e)
    #finally:
        # if conn:
            # conn.close()
    return conn

def insert_url(conn, url):
    """
    Create a new url into the urls table
    :param conn:
    :param url:
    :return: url id
    """
    sql = ''' INSERT INTO urls(url)
              VALUES(?) '''
    cur = conn.cursor()
    cur.execute(sql, [url])
    conn.commit()
    return cur.lastrowid

def db_init(database, images):
    directory = database[0:database.rfind('/')]
    if not os.path.exists(directory):
        print("creating DB at : " + directory)
        os.makedirs(directory)
    conn = create_connection(database)
    # create tables
    with conn:
        # check if table exists
        if (db_t_exists(conn, 'urls') == 0 ):
            # create urls table
            sql_create_urls_table = """ CREATE TABLE IF NOT EXISTS urls (
                                            id integer PRIMARY KEY,
                                            url text NOT NULL
                                        ); """
            db_execute(conn, sql_create_urls_table)
            db_prefill(database, images)

def db_prefill(database, images):
    for timg in images:
        db_insert(database, timg)

    
def db_insert(database, url):
    conn = create_connection(database)
    with conn:
        # insert a url
        url_id = insert_url(conn, url)
        print("new url in ", url_id)
    

def db_replace(database, url):
    conn = create_connection(database)
    with conn:
        cur = conn.cursor()
        tcmd = ("SELECT EXISTS (SELECT name FROM sqlite_schema WHERE type='index' AND name='idx_url');")
        cur = db_execute(conn, tcmd)
        if (cur.fetchone()[0]!=1):
            sql = "CREATE UNIQUE INDEX idx_url ON urls(id);"
            cur.execute(sql)
            conn.commit()
        # replace inside current values
        tdb = db_fetch_all(database)
        db_replace.counter = (db_replace.counter % len(tdb))+1
        sql = ("REPLACE INTO urls(id, url) VALUES (" + str(db_replace.counter) + ", ?); ")
        cur.execute(sql, [url])        
        conn.commit()
db_replace.counter = 0

def db_fetch_all(database):
    conn = create_connection(database)
    with conn:
        cur = db_execute(conn, "SELECT url from urls")
        #cur = db_execute(conn, "SELECT id from urls")
        test = cur.fetchall()
        return test

def db_t_exists(conn, tname):
    tcmd = ("SELECT EXISTS (SELECT name FROM sqlite_schema WHERE type='table' AND name='" + tname + "');")
    cur = db_execute(conn, tcmd)
    return (cur.fetchone()[0]==1)

def db_t_empty(conn, tname):
    db_execute(conn, ("DELETE FROM '" + tname + "'"))

