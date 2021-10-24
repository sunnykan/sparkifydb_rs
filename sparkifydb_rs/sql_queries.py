import configparser


# CONFIG
config = configparser.ConfigParser()
config.read("dwh.cfg")

# DROP TABLES

staging_events_table_drop = ""
staging_songs_table_drop = "drop table if exists staging_songs"
songplay_table_drop = ""
user_table_drop = ""
song_table_drop = ""
artist_table_drop = ""
time_table_drop = ""

# CREATE TABLES

staging_events_table_create = """create table staging_events (
    artist varchar,
    auth varchar,
    firstName varchar,
    gender varchar,
    iteminSession smallint,
    lastName varchar,
    length double precision,
    level varchar,
    location varchar,
    method varchar,
    page varchar,
    registration bigint,
    sessionid integer,
    song varchar,
    status integer,
    ts bigint,
    userAgent varchar,
    userId integer
);
"""

staging_songs_table_create = """create table staging_songs (
    num_songs smallint,
    artist_id varchar,
    artist_latitude double precision,
    artist_longitude double precision,
    artist_location varchar,
    artist_name varchar,
    song_id varchar,
    title varchar,
    duration double precision,
    year integer
);
"""

songplay_table_create = """create table songplays (
    songplay_id double precision DEFAULT nextval('songplay_seq') not null,
    user_id integer not null,
    song_id varchar,
    artist_id varchar,
    session_id varchar,
    start_time bigint not null,
    level varchar,
    location varchar,
    user_agent varchar,
    primary key (songplay_id),
    foreign key(user_id)
        references users(user_id),
    foreign key(song_id)
        references songs(song_id),
    foreign key(artist_id)
        references artists(artist_id),
    foreign key(start_time)
        references time(start_time)
)
"""

user_table_create = """create table users (
    user_id integer not null,
    first_name varchar,
    last_name varchar,
    gender varchar,
    level text,
    primary key (user_id);
)
"""

song_table_create = """create table songs (
    song_id varchar not null,
    artist_id varchar not null,
    title varchar,
    year integer,
    duration double precision,
    primary key (song_id);
)
"""

artist_table_create = """create table artists (
    artist_id varchar not null,
    name varchar,
    location varchar,
    latitude double precision,
    longitude double precision,
    primary key (artist_id);
)
"""

time_table_create = """create table time (
    start_time bigint not null,
    hour smallint,
    day smallint,
    week smallint,
    month smallint,
    year smallint,
    weekday smallint,
    primary key (start_time);
)
"""

# STAGING TABLES

staging_events_copy = f"""copy staging_events from {config.get("S3", "LOG_DATA")} 
credentials 'aws_iam_role={config.get("IAM_ROLE", "ARN")}' json {config.get("S3", "LOG_JSONPATH")} region 'us-west-2';"""


staging_songs_copy = f"""copy staging_songs from {config.get("S3", "SONG_DATA")} 
credentials 'aws_iam_role={config.get("IAM_ROLE", "ARN")}' json 'auto' region 'us-west-2';"""

# FINAL TABLES

songplay_table_insert = """insert into songplays (user_id,
        song_id,
        artist_id,
        session_id,
        start_time,
        level,
        location,
        user_agent)
    select e.userId, 
        s.song_id, 
        s.artist_id, 
        e.sessionid, 
        e.ts, 
        e.level, 
        e.location, 
        e.userAgent
    from staging_events e
    left join staging_songs s
    on s.title = e.song 
        and s.artist_id = e.artist
            and round(s.duration) = round(e.length)
    where page = 'NextSong';
"""

user_table_insert = """insert into users (user_id, first_name, last_name, gender, level)
    select distinct userId,
                    firstName,
                    lastName,
                    gender,
                    level
    from staging_events
    where page = 'NextSong'
    and user_id NOT IN (select distinct user_id FROM users);
"""

song_table_insert = """insert into songs (song_id, title, artist_id, year, duration)
    select distinct song_id, title, artist_id, year, duration
    from staging_songs;
"""

artist_table_insert = """insert into artists (artist_id, name, location, latitude, longitude)
    select distinct artist_id, artist_name, artist_location, artist_latitude, artist_longitude
    from staging_songs;
"""


time_table_insert = """insert into time(start_time, hour, day, week, month, year, weekday)
    select t.start_time,
            extract(hour from t.ts) as hour,
            extract(day from t.ts) as day,
            extract(week from t.ts) as week,
            extract(month from t.ts) as month,
            extract(isoyear from t.ts) as year,
            extract(isodow from t.ts) as weekday
    from (
        select start_time, TIMESTAMP 'epoch' + start_time/1000 * INTERVAL '1 second' ts 
        from songplays       
    ) t;
"""

# QUERY LISTS

create_table_queries = [
    staging_events_table_create,
    staging_songs_table_create,
    songplay_table_create,
    user_table_create,
    song_table_create,
    artist_table_create,
    time_table_create,
]
drop_table_queries = [
    staging_events_table_drop,
    staging_songs_table_drop,
    songplay_table_drop,
    user_table_drop,
    song_table_drop,
    artist_table_drop,
    time_table_drop,
]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [
    songplay_table_insert,
    user_table_insert,
    song_table_insert,
    artist_table_insert,
    time_table_insert,
]
