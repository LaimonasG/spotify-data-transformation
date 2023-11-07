CREATE OR REPLACE VIEW view1 AS
SELECT
    t.id AS track_id,
    t.name AS track_name,
    t.id_artists AS artist_ids,
    COALESCE(SUM(m.followers),0) AS total_followers
FROM tracks t
LEFT JOIN LATERAL (
    SELECT SUM(a.followers) AS followers
    FROM UNNEST(t.id_artists) AS artist_id
    JOIN artists a ON a.id = artist_id
) m ON true
GROUP BY t.id, t.name, t.id_artists
ORDER BY t.id;