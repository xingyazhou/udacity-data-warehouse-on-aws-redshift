import configparser

# CONFIG

config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = """DROP TABLE IF EXISTS staging_events CASCADE;"""
staging_songs_table_drop = """DROP TABLE IF EXISTS staging_songs CASCADE;"""
songplay_table_drop = """DROP TABLE IF EXISTS songplays CASCADE;"""
user_table_drop = """DROP TABLE IF EXISTS users CASCADE;"""
song_table_drop = """DROP TABLE IF EXISTS songs CASCADE;"""
artist_table_drop = """DROP TABLE IF EXISTS artists CASCADE;"""
time_table_drop = """DROP TABLE IF EXISTS time CASCADE;"""

# CREATE TABLES

staging_events_table_create= ("""
CREATE TABLE IF NOT EXISTS staging_events
(
    artist_name      varchar(MAX),
    auth             varchar(MAX),
    first_name       varchar(MAX),
    gender           varchar(MAX),
    item_in_session  int4,
    last_name        varchar(MAX),
    length           numeric,
    level            varchar(MAX),
    location         varchar(MAX),
    method           varchar(MAX),
    page             varchar(MAX),
    registration     varchar(MAX),
    sesion_id        int4,
    song             varchar(MAX),
    status           varchar(MAX),
    ts               int8,
    user_agent       varchar(MAX),
    user_id          varchar(MAX)
)
""")

staging_songs_table_create = ("""
CREATE TABLE IF NOT EXISTS staging_songs
(
    num_songs        int,
    artist_id        varchar(MAX),
    artist_latitude  numeric,
    artist_longitude numeric,
    artist_location  varchar(MAX),
    artist_name      varchar(MAX),
    song_id          varchar(MAX),
    title            varchar(MAX),
    duration         numeric,
    year             int
)
""")

songplay_table_create = ("""
CREATE TABLE IF NOT EXISTS songplays(
    songplay_id      int8 IDENTITY PRIMARY KEY,
    start_time       timestamp REFERENCES time(start_time) NOT NULL,
    user_id          varchar(18) REFERENCES users(user_id) NOT NULL,
    level            varchar(4),
    song_id          varchar(18) REFERENCES songs(song_id)  NOT NULL,
    artist_id        varchar(18) REFERENCES artists(artist_id)  NOT NULL,
    session_id       int4,
    location         varchar(1024),
    user_agent       varchar(1024),
    CONSTRAINT       uniqueness UNIQUE ("start_time","user_id")
)
DISTKEY (songplay_id)
SORTKEY (user_id, song_id, artist_id)
;
""")


user_table_create = ("""
CREATE TABLE IF NOT EXISTS users(
    user_id         varchar(18) PRIMARY KEY,
    first_name      varchar(128),
    last_name       varchar(128),
    gender          char,
    level           varchar(4)    
)
DISTKEY (user_id)
SORTKEY (user_id)
;
""")

song_table_create = ("""
CREATE TABLE IF NOT EXISTS songs(
    song_id        varchar(18) PRIMARY KEY,
    title          varchar(1024),
    artist_id      varchar(1024),
    year           int4,
    duration       float8
)
DISTKEY (song_id)
SORTKEY (artist_id)   
;
""")

artist_table_create = ("""
CREATE TABLE IF NOT EXISTS artists(
    artist_id   varchar(18) PRIMARY KEY,
    name        varchar(1024),
    location    varchar(1024),
    latitude    float8,
    longitude   float8    
)
DISTKEY (artist_id)
SORTKEY (artist_id)
;
""")

time_table_create = ("""
CREATE TABLE IF NOT EXISTS time(
    start_time  timestamp PRIMARY KEY,
    hour        int4,
    day         int4,
    week        int4,
    month       int4,
    year        int4,
    weekday     int4    
)
DISTKEY (start_time)
SORTKEY (start_time, year, month, week, day, weekday, hour)
;
""")

# STAGING TABLES

iam = config.get('IAM', 'IAM_ROLE')
song_data = config.get('S3', 'SONG_DATA')
log_data = config.get('S3','LOG_DATA')
log_json_data = config.get('S3', 'LOG_JSONPATH')

staging_events_copy = ("""
COPY staging_events 
FROM {}
IAM_ROLE {}
JSON {}
""").format(log_data, iam, log_json_data)

staging_songs_copy = ("""
COPY staging_songs
FROM {}
IAM_ROLE {}
JSON {}
""").format(song_data, iam, "'auto'")


# FINAL TABLES

songplay_table_insert = ("""
INSERT INTO songplays(
    start_time,
    user_id,
    level,
    song_id,
    artist_id,
    session_id,
    location,
    user_agent
    )
""")


songplay_table_insert = ("""
INSERT INTO songplays (
    start_time,
    user_id,
    level,
    song_id,
    artist_id,
    session_id,
    location,
    user_agent
)

WITH song_artist as (
SELECT 
    songs.artist_id,
    artists.name as artist_name,
    songs.title as song_name,
    songs.song_id as song_id,
    songs.duration as duration
FROM songs
INNER JOIN artists using (artist_id)
)
                         
SELECT
    date_add('ms', ts, '1970-01-01') as start_time,
    user_id,
    level,
    sa.song_id,
    sa.artist_id,
    sesion_id,
    location,
    user_agent
  FROM staging_events se
  INNER JOIN song_artist as sa 
  ON sa.artist_name=se.artist_name and sa.song_name=se.song AND sa.duration=se.length
""")

song_table_insert = ("""
INSERT INTO songs(
    song_id,
    title,
    artist_id,
    year,
    duration
    )

SELECT 
    song_id,
    title,
    artist_id,
    year,
    duration
FROM staging_songs
WHERE song_id NOT IN (SELECt song_id FROM songs)
""")

artist_table_insert = ("""
INSERT INTO artists(
    artist_id,
    name,
    location,
    latitude,
    longitude   
    )
    
SELECT     
    artist_id,
    artist_name,
    artist_location,
    max(cast("artist_latitude" as float8)) AS "artist_latitude",
    max(cast("artist_longitude" as float8)) AS "artist_longitude"
FROM staging_songs
WHERE artist_id NOT IN (SELECt artist_id FROM artists)
GROUP BY 1,2,3
""")

user_table_insert = ("""
INSERT INTO users(
    user_id,
    first_name, 
    last_name, 
    gender, 
    level
    )
  
SELECT 
    user_id,
    first_name,
    last_name,
    gender,
    level
FROM staging_events
NATURAL INNER JOIN (SELECT user_id, max(ts) as ts FROM staging_events GROUP BY 1) as last_update
WHERE user_id not in (SELECT user_id from users)
""")

time_table_insert = ("""
INSERT INTO time(
    start_time,
    hour,
    day,
    week,
    month,
    year,
    weekday   
    )

WITH times AS(
    SELECT DATEADD('ms', ts, '19700101') as start_time
    FROM staging_events
    GROUP BY 1
)

SELECT
    start_time,
    EXTRACT (hour FROM start_time) as hour,
    EXTRACT (day FROM start_time) as day,
    EXTRACT (week FROM start_time) as week,
    EXTRACT (month FROM start_time) as month,
    EXTRACT (year FROM start_time) as year,
    EXTRACT (weekday FROM start_time) as weekday
FROM times
WHERE start_time NOT IN (SELECT start_time from time)
""")


# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create,  user_table_create, song_table_create, artist_table_create, time_table_create, songplay_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]

#create_table_queries = [user_table_create, song_table_create, artist_table_create, time_table_create, songplay_table_create]
#drop_table_queries = [ user_table_drop, song_table_drop, artist_table_drop, time_table_drop,songplay_table_drop]

copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [user_table_insert, song_table_insert, artist_table_insert, time_table_insert, songplay_table_insert]
#insert_table_queries = [ user_table_insert, song_table_insert, artist_table_insert, time_table_insert, songplay_table_insert]
