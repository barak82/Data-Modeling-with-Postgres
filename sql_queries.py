# CREATE TABLES
songplays_table_create=("""CREATE TABLE IF NOT EXISTS songplays
                                (
                                songplay_id SERIAL PRIMARY KEY,
                                start_time float,
                                user_id int,
                                level varchar,
                                artist_id varchar,
                                session_id int,
                                location varchar,
                                user_agent varchar
                                )
                        """)

user_table_create=("""CREATE TABLE IF NOT EXISTS users 
                            (
                            user_id int PRIMARY KEY NOT NULL,
                            first_name varchar NOT NULL,
                            last_name varchar NOT NULL,
                            gender varchar,
                            level varchar
                            );
                    """)

song_table_create=("""CREATE TABLE IF NOT EXISTS songs
                            (
                            song_id varchar PRIMARY KEY,
                            title varchar,
                            artist_id varchar,
                            year int,
                            duration float
                            )
                    """)

artist_table_create=("""CREATE TABLE IF NOT EXISTS artists
                                (
                                artist_id varchar PRIMARY KEY NOT NULL,
                                name varchar NOT NULL,
                                location varchar NOT NULL,
                                latitude int,
                                longitude int
                                )
                        """)

time_table_create=("""CREATE TABLE IF NOT EXISTS time 
                            (
                            start_time varchar PRIMARY KEY NOT NULL,
                            hour int NOT NULL,
                            day int NOT NULL,
                            week int NOT NULL,
                            month int NOT NULL,
                            year int NOT NULL,
                            weekday int NOT NULL
                            )
                    """)


# INSERT RECORDS
songplays_table_insert=("""INSERT INTO songplays
                            (                
                                start_time,
                                user_id,
                                level,  
                                artist_id,
                                session_id, 
                                location,
                                user_agent) 
                           VALUES (%s, %s,%s, %s,%s, %s,%s
                           )
                """)                                            

user_table_insert=("""INSERT INTO users
                            (
                            user_id, 
                            first_name,
                            last_name,
                            gender, 
                            level)
                       VALUES (%s, %s, %s,%s, %s)
                       ON CONFLICT (user_id)
                       DO UPDATE SET 
                               (level) = 
                               (EXCLUDED.level)
                    """)

song_table_insert=("""INSERT INTO songs (
                            song_id,
                            title,
                            artist_id,
                            year,
                            duration) 
                            VALUES (%s, %s, %s, %s, %s)
                      ON CONFLICT (song_id)
                      DO UPDATE SET (
                          song_id,
                          title,
                          artist_id,
                          year,
                          duration) = (
                          EXCLUDED.song_id,
                          EXCLUDED.title,
                          EXCLUDED.artist_id,
                          EXCLUDED.year,
                          EXCLUDED.duration);""")

artist_table_insert=("""INSERT INTO artists (
                            artist_id,
                            name,
                            location,
                            latitude,
                            longitude)
                    VALUES (%s, %s, %s,%s,%s)
                    ON CONFLICT (artist_id)
                    DO UPDATE SET (
                            artist_id,
                            name,
                            location,
                            latitude,
                            longitude) = (
                            EXCLUDED.artist_id,
                            EXCLUDED.name,
                            EXCLUDED.location,
                            EXCLUDED.latitude,
                            EXCLUDED.longitude);""")

time_table_insert=("""INSERT INTO time (
                            start_time,
                            hour,
                            day,
                            week,
                            month,
                            year,
                            weekday)
                      VALUES (%s, %s, %s,%s, %s, %s,%s)
                      ON CONFLICT (start_time)
                      DO UPDATE SET (
                          start_time,
                          hour,
                          day,
                          week,
                          month,
                          year,
                          weekday) =(
                          EXCLUDED.start_time,
                          EXCLUDED.hour,
                          EXCLUDED.day,
                          EXCLUDED.week,
                          EXCLUDED.month,
                          EXCLUDED.year,
                          EXCLUDED.weekday);""")

#drop tables 
songplays_table_drop="DROP table songplays"
user_table_drop="DROP table users"
song_table_drop="DROP table songs"
artist_table_drop="DROP table artists"
time_table_drop="DROP table time"

# FIND SONGS
def find_songs(atributes,table_db):
    try: 
        cur.execute("SELECT"+atributes+ "FROM"+ table_db + ";")
    except psycopg2.Error as e: 
        print("Error: select *")
        print (e)

# SELECT SONGS                
song_select="SELECT song_id, artist_id FROM songs where title = %s and duration = %s"
artist_select="SELECT artist_id, name, location FROM artists where name = %s"
user_select="SELECT level, FROM users where user_id = %s"
# QUERY LISTS
create_table_queries = [songplays_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [songplays_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]