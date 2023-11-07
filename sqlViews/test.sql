-- INSERT INTO artists (id, followers, name, popularity)
-- VALUES ('45tIt06XoI0Iio4LBEVpls', 20, 'Armid & Amir Zare Pashai feat. Sara Rouzbehani', 0);

-- SELECT t.id, t.name, COALESCE(SUM(t2.followers) OVER (PARTITION BY t.id), 0) AS followers_sum
-- FROM tracks t
-- LEFT JOIN artists t2 ON t2.id = '45tIt06XoI0Iio4LBEVpls';
-- limit 10

-- UPDATE tracks
-- SET id_artists = array_append(id_artists, '034I93yj4oItrwW0YK6HWp')
-- WHERE id = '00dYkxYZB1sChcRL6aUK85';

-- select * from tracks
-- WHERE id = '00dYkxYZB1sChcRL6aUK85';

-- SELECT ta.track_id, SUM(a.followers) AS total_f
-- from tracks t
-- FROM (
--     SELECT UNNEST(id_artists) AS artist_id, id AS track_id
--     FROM tracks
-- ) ta
-- JOIN artists a ON ta.artist_id = a.id
-- GROUP BY ta.track_id
-- HAVING SUM(a.followers) > 0;

-- CREATE OR REPLACE VIEW view1 AS
-- SELECT
--     t.id AS track_id,
--     t.name AS track_name,
--     t.popularity AS popularity,
--     t.energy AS energy,
--     t.danceability AS danceability,
--     COALESCE(SUM(a.total_f), 0) AS total_followers
-- FROM tracks t
-- LEFT JOIN (
--     SELECT ta.track_id, SUM(a.followers) AS total_f
--     FROM (
--         SELECT UNNEST(id_artists) AS artist_id, id AS track_id
--         FROM tracks
--     ) ta
--     JOIN artists a ON ta.artist_id = a.id
--     GROUP BY ta.track_id
--     HAVING SUM(a.followers) > 0
-- ) a ON a.track_id = t.id
-- GROUP BY t.id, t.name, t.id_artists
-- ORDER BY t.id

SELECT
    t.id AS track_id,
    t.name AS track_name,
    t.id_artists AS artist_ids,
    COALESCE(SUM(a.followers), 0) AS total_followers
FROM tracks t
LEFT JOIN LATERAL (
    SELECT SUM(a.followers) AS followers
    FROM UNNEST(t.id_artists) AS artist_id
    JOIN artists a ON a.id = artist_id
) a ON true
WHERE t.id = '35iwgR4jXetI318WEWsa1Q'
GROUP BY t.id, t.name, t.id_artists;

-- select 
--     a.id AS id,
--     a.followers AS followers
--     from artists a
--     where a.id='0DheY5irMjBUeLybbCUEZ2' 