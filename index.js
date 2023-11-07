import { readCsvData} from "./src/inOutUtils.js"
import {
   filterTrackData,
   filterArtistData,
   explodeTrackReleaseDate,
   transformTrackDanceability,
   cleanupData
   } from "./src/helperMethods.js"
import {
  connectToAuroraPostgresql,
  disconnectToAuroraPostgresql,
  insertArtistData,
  insertTrackData,
  createView,
  testView,
  deleteAll
} from "./src/databaseUtils.js"
import {downloadCSVDataFromS3,uploadToS3Bucket} from "./src/s3Utils.js"

const artist1FilePathTest = `myData/artists.csv`;
const track1FilePathTest = `myData/tracks.csv`;

const artist1Filename = "artists1.csv"
const track1Filename = "tracks1.csv"
const artist2Filename = "artists2.csv"
const track2Filename = "tracks2.csv"
const artist1FilePath = `data/${artist1Filename}`;
const track1FilePath = `data/${track1Filename}`;
const artist2FilePath = `data/${artist2Filename}`;
const track2FilePath = `data/${track2Filename}`;

const viewFolderName="sqlViews"
const view1Name="view1"
const view2Name="view2"
const view3Name="view3"

//if value set to null, will read whole file
const linesToRead = null;
const insertBatchSize = 5;

main()

async function main() {
  readCsvData(track1FilePathTest, linesToRead, (error, trackData, trackHeader) => {
    if (error) {
      console.error('Error:', error);
    } else {
      console.log("Track data length before filtering: ", trackData.length)

      const filteredTracks = filterTrackData(trackData)

      console.log("Track data length after filtering: ", filteredTracks.length)

      const explodedTrackData = explodeTrackReleaseDate(filteredTracks)

      const transformedTrackDanceability = transformTrackDanceability(explodedTrackData);

      const cleanTrackData = cleanupData(transformedTrackDanceability, 5);

      readCsvData(artist1FilePathTest, linesToRead, async (error, artistData, artistHeader) => {
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

          await uploadToS3Bucket(cleanTrackData, track1Filename)
          await uploadToS3Bucket(cleanArtistData, artist1Filename)

          try {
            await connectToAuroraPostgresql();

            await deleteAll()

            if (cleanArtistData.length >0) {
              const dataToInsert = cleanArtistData.slice(1)
              await insertArtistData(dataToInsert, insertBatchSize)
            }
            else
              console.log("No artists to insert.")

            if (cleanTrackData.length > 0) {
              const dataToInsert = cleanTrackData.slice(1)
              await insertTrackData(dataToInsert, insertBatchSize)
            } else
              console.log("No tracks to insert.")


           console.log("Testing the first view: ")
           await createView(`${viewFolderName}/${view1Name}.sql`)
          await testView(view1Name)

           console.log("Testing the second view: ")
           await createView(`${viewFolderName}/${view2Name}.sql`)
           await testView(view2Name)

           console.log("Testing the third view: ")
           await createView(`${viewFolderName}/${view3Name}.sql`)
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