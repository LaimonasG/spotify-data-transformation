import 'dotenv/config'
import pg from "pg"
import fs from "fs"
import pgPromise  from 'pg-promise';

const cn = {
  host: process.env.AWS_AURORA_ENDPOINT,
  port: process.env.AWS_AURORA_PORT,
  database: process.env.AWS_AURORA_DATABASE,
  user: process.env.AWS_AURORA_USERNAME,
  password: process.env.AWS_AURORA_PASSWORD,
  max: 30,
  ssl: {
    // Set the SSL mode
    mode: 'prefer',
    rejectUnauthorized: false, // Options: 'disable', 'require', 'prefer', 'allow'
  },
};

const pgp = pgPromise(/* options */);
const db = pgp(cn);


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

export async function insertArtistData(data, batchSize) {
  try {
    console.time('Insert artist data');
    let acc = 0;

    let batchCount = 0;
    const artistBatch = [];
    const genreBatch = [];

    console.log(`Inserting artist data. Batch size: ${batchSize}:`)

    for (const a of data) {
      const artist = {
        id: a[0],
        followers: parseFloat(a[1]),
        name: a[3],
        popularity: parseInt(a[4])
      };

      artistBatch.push(artist);
      acc += 1;

      const genres = a[2];

      if(genres!== "[]"){
        const replaceSingleQuotes = genres.replace(/'/g, '"').trim();
        const replaceInsideQuotes=replaceSingleQuotes.replace(/(?<!\[|,#|\s)"(?!\]|,)/g,`'`)
        const replaceNScenario=replaceInsideQuotes.replace(/"n'/g,`'n'`)
        try{
          const genreArray = JSON.parse(replaceNScenario);
          if (Array.isArray(genreArray) && genreArray.length > 0) {
            for (const g of genreArray) {
              const genre = {
                genre_name: g,
                artistId: artist.id
              };
              genreBatch.push(genre);
            }
          }
        }catch(error){
          console.log("err: ", error)
          console.log("err data: ", replaceNScenario)
        }       
      }

      if (acc % batchSize === 0) {
        batchCount = batchCount + 1
        await insertArtists(artistBatch, batchCount);

        if(genreBatch.length>0){
          await insertGenres(genreBatch, batchCount);
        }

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

    console.timeEnd('Insert artist data');
    console.log("Inserted artists count:", acc);
  } catch (error) {
    console.error('Error inserting artist data', error);
    throw error;
  }
}

async function insertArtists(artists, batchCount) {
    await db.tx(async (t) => {
      const insertQuery = pgp.helpers.insert(artists, ['id', 'followers', 'name', 'popularity'], 'artists');

      await t.any(insertQuery);      

    }).then(data => {
      console.log(`Batch ${batchCount} inserted successfully to artist table`);
  })
  .catch(error => {
    console.error(`Error inserting artists in batch ${batchCount}`, error);
    throw error;
  });  
}

async function insertTracks(tracks, batchCount) {
  await db.tx(async (t) => {
    const insertQuery = pgp.helpers.insert(tracks, [
      'id', 'name', 'popularity', 'duration_ms', 'explicit', 'id_artists', 'year', 'month', 'day',
      'danceability', 'energy', 'key', 'loudness', 'mode', 'speechiness', 'acousticness',
      'instrumentalness', 'liveness', 'valence', 'tempo', 'time_signature'
    ], 'tracks') + 'RETURNING id';

    await t.any(insertQuery);

  }).then(data => {
    console.log(`Batch ${batchCount} inserted successfully to track table`);
})
.catch(error => {
  console.error(`Error inserting tracks in batch ${batchCount}`, error);
  throw error;
}); 
}

async function insertGenres(genres) {
  await db.tx(async (t) => {
      const genreNames = genres.map((x) => x.genre_name);

      const insertGenresQuery = pgp.helpers.insert(
        genreNames.map((name) => ({ genre_name: name })),
        ['genre_name'],
        'genres'
      ) + ' RETURNING genre_id';
      const genreIds = await t.any(insertGenresQuery);

      const artistIds = genres.map((x) => x.artistId);

      const genreArtistsData = genreIds.map((genreId, index) => ({
        artist_id: artistIds[index],
        genre_id: genreId.genre_id,
      }));

      const insertGenreArtistsQuery = pgp.helpers.insert(
        genreArtistsData,
        ['artist_id', 'genre_id'],
        'artist_genres'
      );
      await t.any(insertGenreArtistsQuery);

  }).then()
.catch(error => {
  console.error(`Error inserting genres.`, error);
  throw error;
}); 
}

export async function insertTrackData(data, batchSize) {
  try {
    console.time('Insert track data');
    let acc = 0

    let batchCount = 0;
    const trackBatch = [];

    console.log(`Inserting track data. Batch size: ${batchSize}:`)

    for (const t of data) {
      const track = {
        id: t[0],
        name: t[1],
        popularity: parseInt(t[2]),
        duration_ms: parseInt(t[3]),
        explicit: parseInt(t[4]),
        id_artists: "{" + t[6] + "}",
        year: parseInt(t[7]),
        month: parseInt(t[8]),
        day: parseInt(t[9]),
        danceability: parseFloat(t[10]),
        energy: parseFloat(t[11]),
        key: parseInt(t[12]),
        loudness: parseFloat(t[13]),
        mode: parseInt(t[14]),
        speechiness: parseFloat(t[15]),
        acousticness: parseFloat(t[16]),
        instrumentalness: parseFloat(t[17]),
        liveness: parseFloat(t[18]),
        valence: parseFloat(t[19]),
        tempo: parseFloat(t[20]),
        time_signature: parseInt(t[21])
      };

      trackBatch.push(track);
      acc += 1;

      if (acc % batchSize === 0) {
        batchCount = batchCount + 1
        await insertTracks(trackBatch,batchCount)
        trackBatch.length = 0;
      }
    };

    console.timeEnd('Insert track data');
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
    const query4 = `TRUNCATE TABLE artist_genres`;
    await pgClient.query(query4);

    const query3 = `TRUNCATE TABLE genres CASCADE`;
    await pgClient.query(query3);

    const query1 = `TRUNCATE TABLE artists CASCADE`;
    await pgClient.query(query1);

    const query2 = `TRUNCATE TABLE tracks`;
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

export async function altertable() {
  try {
    const query = `ALTER TABLE artist_genres
    ADD COLUMN id SERIAL PRIMARY KEY;`;

    const result=await pgClient.query(query);

    } catch (error) {
   // console.error(`Error while displaying view ${viewName}: `, error);
    throw error;
  }
}