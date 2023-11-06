CREATE OR REPLACE VIEW view3 AS
WITH RankedTracks AS (
    SELECT
        t.id AS track_id,
        t.name AS track_name,
        t.year AS release_year,
        t.energy AS track_energy,
        ROW_NUMBER() OVER (PARTITION BY t.year ORDER BY t.energy DESC) AS rn
    FROM tracks t
)
SELECT
    track_id,
    track_name,
    release_year,
    track_energy
FROM RankedTracks
WHERE rn = 1