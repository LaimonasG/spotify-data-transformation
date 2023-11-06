import 'dotenv/config'
import { Upload } from "@aws-sdk/lib-storage";
import pg from "pg"
import fastcsv from "fast-csv"
import fs from "fs"

const pgClient = new pg.Client({
  host: process.env.AWS_AURORA_ENDPOINT,
  port: process.env.AWS_AURORA_PORT,
  user: process.env.AWS_AURORA_USERNAME,
  password: process.env.AWS_AURORA_PASSWORD,
  database: process.env.AWS_AURORA_DATABASE,
  ssl: {
    rejectUnauthorized: false,
  },
})


export async function connectToAuroraPostgresql() {
  try {
    await pgClient.connect();
    console.log('Connected to AWS Aurora PostgreSQL');
  } catch (error) {
    console.error('Error connecting to AWS Aurora PostgreSQL:', error);
    throw error;
  }
}

export async function disconnectToAuroraPostgresql() {
  try {
    await pgClient.end();
    console.log('Disconnected from AWS Aurora PostgreSQL');
  } catch (error) {
    console.error('Error disconnecting from AWS Aurora PostgreSQL:', error);
    throw error;
  }
}

export async function insertGenre(genreName, artistId) {
  try {
    const insertGenreQuery = `
      INSERT INTO genres (genre_name)
      VALUES ($1)
      RETURNING genre_id`;

    const genreResult = await pgClient.query(insertGenreQuery, [
      genreName,
    ]);

    const insertArtist_genresQuery = `
    INSERT INTO artist_genres (artist_id,genre_id)
    VALUES ($1,$2)
    RETURNING artist_id,genre_id`;

    await pgClient.query(insertArtist_genresQuery, [
      artistId,
      genreResult.rows[0].genre_id
    ]);

  } catch (error) {
    console.error('Error inserting genre data', error);
    throw error;
  }
}

export async function insertArtistData(data, batchSize) {
  try {
    await pgClient.query("BEGIN"); 
    let acc = 0;

    let batchCount = 0;
    const artistBatch = [];
    const genreBatch = [];

    console.log(`Inserting artist data. Batch size: ${batchSize}:`)

    for (const a of data) {
      const artist = {
        id: a[0],
        followers: a[1],
        name: a[3],
        popularity: a[4]
      };

      artistBatch.push(artist);
      acc += 1;

      const genres = a[2];

      if (genres.length > 0) {
        for (const g of genres) {
          const genre = {
            genre_name: g,
            artistId: artist.id
          };
          genreBatch.push(genre);
        }
      }

      if (acc % batchSize === 0) {
        batchCount = batchCount + 1
        await insertArtists(artistBatch, batchCount);

        if(genreBatch.length>0)
        await insertGenres(genreBatch, batchCount);

        // Clear the batches
        artistBatch.length = 0;
        genreBatch.length = 0;
      }
    }

    // Insert any remaining artists and genres
    if (artistBatch.length > 0) {
      acc+=artistBatch.length;
      batchCount = batchCount + 1
      await insertArtists(artistBatch,batchCount);
    }
    if (genreBatch.length > 0) {
      await insertGenres(genreBatch);
    }

    console.log("Inserted artists count:", acc);
    await pgClient.query("COMMIT");

  } catch (error) {
    console.error('Error inserting artist data', error);
    await pgClient.query("ROLLBACK");
    throw error;
  }
}

async function insertArtists(artists, batchCount) {
  try {
    const query = `
      INSERT INTO artists (id, followers, name, popularity)
      VALUES ($1, $2, $3, $4)`;

    for (const artist of artists) {
      const values = [artist.id, artist.followers, artist.name, artist.popularity];
      await pgClient.query(query, values);
    }

    console.log(`Batch ${batchCount} inserted succesfully to artist table`)

  } catch (error) {
    console.error('Error inserting artists', error);
    throw error;
  }
}

async function insertTracks(tracks, batchCount) {
  try {
    const insertQuery = `
      INSERT INTO tracks (id, name, popularity, duration_ms,explicit,id_artists,year,month,day,danceability,
        energy,key,loudness,mode,speechiness,acousticness,instrumentalness,liveness,valence,tempo,time_signature)
      VALUES ($1, $2, $3,$4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20, $21)
      RETURNING id`;

    for (const track of tracks) {
      const values = [
        track.id,
        track.name,
        parseInt(track.popularity),
        parseInt(track.duration_ms),
        parseInt(track.explicit),
        track.id_artists,
        parseInt(track.year),
        parseInt(track.month),
        parseInt(track.day),
        track.danceability,
        parseFloat(track.energy),
        parseInt(track.key),
        parseFloat(track.loudness),
        parseInt(track.mode),
        parseFloat(track.speechiness),
        parseFloat(track.acousticness),
        parseFloat(track.instrumentalness),
        parseFloat(track.liveness),
        parseFloat(track.valence),
        parseFloat(track.tempo),
        parseInt(track.time_signature)
      ];
      await pgClient.query(insertQuery, values);
    }

    console.log(`Batch ${batchCount} inserted succesfully to track table`)

  } catch (error) {
    console.error('Error inserting tracks', error);
    throw error;
  }
}

async function insertGenres(genres) {
  try {
    const insertGenreQuery = `
      INSERT INTO genres (genre_name)
      VALUES ($1)
      RETURNING genre_id`;

    const insertArtistGenresQuery = `
      INSERT INTO artist_genres (artist_id, genre_id)
      VALUES ($1, $2)`;

    for (const gen of genres) {
      const genreResults = await pgClient.query(insertGenreQuery, [gen.name]);
      const artistId = gen.artistId;
      const genreId = genreResults.rows[0].genre_id;
      await pgClient.query(insertArtistGenresQuery, [artistId, genreId]);
    }

  } catch (error) {
    console.error('Error inserting genres', error);
    throw error;
  }
}

export async function insertTrackData(data, batchSize) {
  try {
    await pgClient.query("BEGIN")
    let acc = 0

    let batchCount = 0;
    const trackBatch = [];

    console.log(`Inserting track data. Batch size: ${batchSize}:`)

    for (const t of data) {
      const track = {
        id: t[0],
        name: t[1],
        popularity: t[2],
        duration_ms: t[3],
        explicit: t[4],
        id_artists: "{" + t[6] + "}",
        year: t[7],
        month: t[8],
        day: t[9],
        danceability: t[10],
        energy: t[11],
        key: t[12],
        loudness: t[13],
        mode: t[14],
        speechiness: t[15],
        acousticness: t[16],
        instrumentalness: t[17],
        liveness: t[18],
        valence: t[19],
        tempo: t[20],
        time_signature: t[21]
      }

      trackBatch.push(track);
      acc += 1;

      if (acc % batchSize === 0) {
        batchCount = batchCount + 1
        await insertTracks(trackBatch,batchCount)
        trackBatch.length = 0;
      }
    };

    console.log("Inserted tracks count:", acc)
    await pgClient.query("COMMIT")

  } catch (error) {
    console.error('Error inserting tracks', error);
    throw error;
  }
}

export async function getArtistsCount() {
  try {
    const query = `SELECT Count(*) FROM artists;`;

    const result = await pgClient.query(query);

    console.log(`Artists: ${result.rows[0].count}`);
  } catch (error) {
    console.error('Error querying artist count', error);
    throw error;
  }
}

export async function getTracks(count) {
  try {
    const query = `SELECT * FROM tracks order by id limit $1;`;
    const result = await pgClient.query(query, [count]);

    console.log('Tracks:');
    result.rows.forEach(row => {
      console.log(row);
    });
  } catch (error) {
    console.error('Error querying track count', error);
    throw error;
  }
}

export async function getArtists(count) {
  try {
    const query = `SELECT * FROM artists order by id limit $1;`;
    const result = await pgClient.query(query, [count]);

    console.log('Artists:');
    result.rows.forEach(row => {
      console.log(row);
    });
  } catch (error) {
    console.error('Error querying artists', error);
    throw error;
  }
}

export async function changeColumnDataType(column, type) {
  try {
    const query = `ALTER TABLE tracks
    ALTER COLUMN ${column}
    SET DATA TYPE ${type};`;
    await pgClient.query(query);

  } catch (error) {
    console.error('Error querying artists', error);
    throw error;
  }
}

export async function getTracksCount() {
  try {
    const query = `SELECT COUNT(*) FROM tracks;`;
    const result = await pgClient.query(query);

    console.log(`Track count: ${result.rows[0].count}`);
  } catch (error) {
    console.error('Error querying track count', error);
    throw error;
  }
}

export async function getGenresCount() {
  try {
    const query = `SELECT COUNT(*) FROM genres;`;
    const result = await pgClient.query(query);

    console.log(`Genres count: ${result.rows[0].count}`);
  } catch (error) {
    console.error('Error querying track count', error);
    throw error;
  }
}

export async function deleteAll() {
  try {
    const query4 = `DELETE from artist_genres`;
    await pgClient.query(query4);

    const query3 = `DELETE from genres`;
    await pgClient.query(query3);

    const query1 = `DELETE from artists`;
    await pgClient.query(query1);

    const query2 = `DELETE from tracks`;
    await pgClient.query(query2);

  } catch (error) {
    console.error('Error querying track count', error);
    throw error;
  }
}

export async function createView(filename) {
  try {
    const sqlQuery = fs.readFileSync(filename, 'utf8');

    await pgClient.query(sqlQuery);

    console.error(`View from file ${filename} created successfuly.`);

    } catch (error) {
      console.error(`Error while creating view from file ${filename}: `, error);
    throw error;
  }
}

export async function selectQuery(filename) {
  try {
    const sqlQuery = fs.readFileSync(filename, 'utf8');

    const result=await pgClient.query(sqlQuery);

    console.log(`Test results:`);
    result.rows.forEach(row => {
      console.log(row);
    });

    } catch (error) {
      console.error(`Error while doing selectfrom file ${filename}: `, error);
    throw error;
  }
}

export async function testView(viewName) {
  try {
    const query = `SELECT * FROM ${pgClient.escapeIdentifier(viewName)}`;

    const result=await pgClient.query(query);


    console.log(`Results of view: ${viewName} `, result.rows)

    } catch (error) {
    console.error(`Error while displaying view ${viewName}: `, error);
    throw error;
  }
}

export async function deleteView(viewName) {
  try {
    const query = `DROP VIEW IF EXISTS ${pgClient.escapeIdentifier(viewName)}`;

    await pgClient.query(query);

    console.log(`Deleted view: ${viewName}`);

  } catch (error) {
    console.error(`Error while deleting view ${viewName}: `, error);
    throw error;
  }
}

// CREATE TABLE artists(
//   id VARCHAR(255) PRIMARY KEY,
//   followers double precision DEFAULT 0.0,
//   name VARCHAR(255),
//   popularity INTEGER DEFAULT 0
// );

// CREATE TABLE genres(
//   genre_id serial PRIMARY KEY,
//   genre_name VARCHAR(255)
// );

// CREATE TABLE artist_genres(
//   artist_id VARCHAR(255),
//   genre_id INT,
//   FOREIGN KEY(artist_id) REFERENCES artists(id),
//   FOREIGN KEY(genre_id) REFERENCES genres(genre_id)
// );

// CREATE TABLE tracks(
//   id VARCHAR(255) PRIMARY KEY,
//   name VARCHAR(255),
//   popularity integer,
//   duration_ms INTEGER DEFAULT 0,
//   explicit integer,
//   id_artists VARCHAR[],
//   year integer,
//   month integer,
//   day integer,
//   danceability VARCHAR(255),
//   energy double precision,
//   key integer,
//   loudness double precision,
//   mode integer,
//   speechiness double precision,
//   acousticness double precision,
//   instrumentalness double precision,
//   liveness double precision,
//   valence double precision,
//   tempo double precision,
//   time_signature integer
// );


