class SqlQueries:
    songplay_table_insert = ("""
        SELECT
                events.start_time, 
                events.userid, 
                events.level, 
                songs.song_id, 
                songs.artist_id, 
                events.sessionid, 
                events.location, 
                events.useragent
                FROM (SELECT TIMESTAMP 'epoch' + ts/1000 * interval '1 second' AS start_time, *
            FROM staging_events
            WHERE page='NextSong') events
            LEFT JOIN staging_songs songs
            ON events.song = songs.title
                AND events.artist = songs.artist_name
                AND events.length = songs.duration
    """)

    user_table_insert = ("""
        SELECT distinct userid, firstname, lastname, gender, level
        FROM staging_events
        WHERE page='NextSong'
    """)

    song_table_insert = ("""
        SELECT distinct song_id, title, artist_id, year, duration
        FROM staging_songs
    """)

    artist_table_insert = ("""
        SELECT distinct artist_id, artist_name, artist_location, artist_latitude, artist_longitude
        FROM staging_songs
    """)

    time_table_insert = ("""
        SELECT start_time, extract(hour from start_time), extract(day from start_time), extract(week from start_time), 
               extract(month from start_time), extract(year from start_time), extract(dayofweek from start_time)
        FROM songplays
    """)
    ###### check queries
    songplays_check_nulls = ("""
        SELECT COUNT(*)
        FROM songplays
        WHERE   songplay_id IS NULL OR
                start_time IS NULL OR
                user_id IS NULL;
    """)

    users_check_nulls = ("""
        SELECT COUNT(*)
        FROM users
        WHERE user_id IS NULL;
    """)

    songs_check_nulls = ("""
        SELECT COUNT(*)
        FROM songs
        WHERE song_id IS NULL;
    """)

    artists_check_nulls = ("""
        SELECT COUNT(*)
        FROM artists
        WHERE artist_id IS NULL;
    """)

    time_check_nulls = ("""
        SELECT COUNT(*)
        FROM time
        WHERE start_time IS NULL;
    """)

    #### check queries:
    songplays_check_count = ("""
        SELECT COUNT(*)
        FROM songplays;
    """)

    users_check_count = ("""
        SELECT COUNT(*)
        FROM users;
    """)

    songs_check_count = ("""
        SELECT COUNT(*)
        FROM songs;
    """)

    artists_check_count = ("""
        SELECT COUNT(*)
        FROM artists;
    """)

    time_check_count = ("""
        SELECT COUNT(*)
        FROM time;
    """)