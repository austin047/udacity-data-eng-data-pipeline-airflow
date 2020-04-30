class SqlQueries:
    songplay_table_insert = ("""
    INSERT INTO {} (
        start_time,
        userid,
        level,
        songid,
        artistid,
        sessionid,
        location,
        user_agent
    ) 
    
        SELECT DISTINCT 
                events.start_time AS start_time, 
                events.userid AS userid, 
                events.level AS level, 
                songs.song_id AS songid, 
                songs.artist_id AS artistid, 
                events.sessionId AS sessionid, 
                events.location AS location, 
                events.useragent AS user_agent
                FROM (SELECT TIMESTAMP 'epoch' + ts/1000 * interval '1 second' AS start_time, *
            FROM staging_events
            WHERE page='NextSong') events
            LEFT JOIN staging_songs songs
            ON events.song = songs.title
                AND events.artist = songs.artist_name
                AND events.length = songs.duration
    """)

    user_table_insert = ("""
    INSERT INTO {} (
        userid,
        first_name,
        last_name,
        gender,
        level
    )
        SELECT distinct userId AS userid, 
        firstName AS first_name, 
        lastName AS last_name, 
        gender AS gender, level AS level
        FROM staging_events
        WHERE page='NextSong'
    """)

    song_table_insert = ("""
    INSERT INTO {} (
        songid,
        title,
        artistid,
        year,
        duration
    )
        SELECT distinct song_id AS songid, title AS title, artist_id AS artistid, year AS year, duration AS duration
        FROM staging_songs
    """)

    artist_table_insert = ("""
     INSERT INTO {} (
        artistid,
        name,
        location,
        latitude,
        longitude
    )
    
        SELECT distinct artist_id AS artistid, artist_name AS name, artist_location AS location, artist_latitude AS latitude, artist_longitude AS longitude
        FROM staging_songs
    """)

    time_table_insert = ("""
    INSERT INTO time (
        start_time,
        hour,
        day,
        week,
        month,
        year,
        weekday
    )
        SELECT start_time AS start_time, extract(hour from start_time) AS hour, extract(day from start_time) AS day, extract(week from start_time) AS week, 
               extract(month from start_time) AS month, extract(year from start_time) as year, extract(dayofweek from start_time) AS weekday
        FROM songplays
    """)