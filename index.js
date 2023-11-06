//psql -h spotifydataset.cohs3ananwph.eu-north-1.rds.amazonaws.com

import { readCsvData, writeCsvData, readFileToBuffer } from "./src/inOutUtils.js"
import { filterTrackData, filterArtistData, explodeTrackReleaseDate, transformTrackDanceability, cleanupData } from "./src/helperMethods.js"
import {
  uploadToS3Bucket,
  connectToAuroraPostgresql,
  disconnectToAuroraPostgresql,
 // insertArtist,
  getArtistsCount,
  downloadCSVDataFromS3,
  insertArtistData,
  insertTrackData,
  insertGenre,
 // insertTrack,
  getTracks,
  deleteAll,
  getTracksCount,
  getGenresCount,
  //  uploadArrayToS3AsCSV,
  createView,
  testView,
  deleteView,
  selectQuery,
  getArtists,
  changeColumnDataType
} from "./src/databaseUtils.js"
import {downloadCSVDataFromS3,uploadToS3Bucket} from "./src/s3Utils.js"
import { Readable } from "stream";


const artistFilename = "dataset1/artists.csv"
const trackFilename = "dataset1/tracks.csv"
const artistFilePath = `data/${artistFilename}`;
const trackFilePath = `data/${trackFilename}`;
const artistResultFilePath = `data/result/${artistFilename}`
const trackResultFilePath = `data/result/${trackFilename}`

const view1Filename="sqlViews/view1.sql"
const view1Name="view1"
const view2Filename="sqlViews/view2.sql"
const view2Name="view2"
const view3Filename="sqlViews/view3.sql"
const view3Name="view3"

//if value set to null, will read whole file
const linesToRead = 1000;
const insertBatchSize = 100

main()

async function main() {
  readCsvData(trackFilePath, linesToRead, (error, trackData, trackHeader) => {
    if (error) {
      console.error('Error:', error);
    } else {
      console.log("Track data length before filtering: ", trackData.length)

      const filteredTracks = filterTrackData(trackData)

      console.log("Track data length after filtering: ", filteredTracks.length)

      const explodedTrackData = explodeTrackReleaseDate(filteredTracks)

      const transformedTrackDanceability = transformTrackDanceability(explodedTrackData);

      const cleanTrackData = cleanupData(transformedTrackDanceability, 5);

      readCsvData(artistFilePath, linesToRead, async (error, artistData, artistHeader) => {
        if (error) {
          console.error('Error:', error);
        } else {
          console.log("Artist data length before filtering: ", artistData.length)

          const filteredArtists = filterArtistData(filteredTracks, artistData)

          console.log("Artist data length after filtering: ", filteredArtists.length)

          const cleanArtistData = cleanupData(artistData, 2);

          const flatTrackHeader = Object.values(trackHeader.flat());
          flatTrackHeader.splice(7, 1, "year", "month", "day")
          cleanTrackData.unshift(flatTrackHeader)

          const flatArtistHeader = Object.values(artistHeader.flat());
          cleanArtistData.unshift(flatArtistHeader)

          await uploadToS3Bucket(cleanTrackData, trackFilename)
          await uploadToS3Bucket(cleanArtistData, artistFilename)

          try {
            await connectToAuroraPostgresql();

           // await  changeColumnDataType('danceability','VARCHAR')
           // await deleteAll()

            if (cleanArtistData.length <0) {
              const dataToInsert = cleanArtistData.slice(1)
              await insertArtistData(dataToInsert, insertBatchSize)
            }
            else
              console.log("No artists to insert.")

            if (cleanTrackData.length < 0) {
              const dataToInsert = cleanTrackData.slice(1)
              await insertTrackData(dataToInsert, insertBatchSize)
            } else
              console.log("No tracks to insert.")

           await createView(view3Filename)
           await testView(view3Name)

          } catch (error) {
            console.error('An error occurred:', error);
          } finally {
            await disconnectToAuroraPostgresql();
          }

        }

        //LOAD DATA FROM S3 BUCKET TO AURORA RDS

        //  const tracks = await downloadCSVDataFromS3("result/", "tracks", true, false)
        //  const artists = await downloadCSVDataFromS3("result/", "artists", true, false)

        //  console.log("Art count: ", artists.length)
        //  console.log("Track count: ", tracks.length)


      });
    }
  });
}