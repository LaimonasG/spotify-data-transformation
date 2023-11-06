CREATE OR REPLACE VIEW view1 AS
SELECT
    t.id AS track_id,
    t.name AS track_name,
    t.popularity AS popularity,
    t.energy AS energy,
    t.danceability AS danceability,
    COALESCE(SUM(a.total_f), 0) AS total_followers
FROM tracks t
LEFT JOIN (
    SELECT ta.track_id, SUM(a.followers) AS total_f
    FROM (
        SELECT UNNEST(id_artists) AS artist_id, id AS track_id
        FROM tracks
    ) ta
    JOIN artists a ON ta.artist_id = a.id
    GROUP BY ta.track_id
) a ON a.track_id = t.id
GROUP BY t.id, t.name, t.id_artists
ORDER BY t.id