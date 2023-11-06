import { S3Client, ListObjectsCommand, GetObjectCommand } from "@aws-sdk/client-s3";


const s3Client = new S3Client({
    region: process.env.S3_REGION,
    credentials: {
      accessKeyId: process.env.AWS_ACCESS_KEY_ID,
      secretAccessKey: process.env.AWS_SECRET_ACCESS_KEY,
    },
  });

  export async function uploadToS3Bucket(array, filename) {
    const now = new Date();
    const formattedDate = now.toLocaleString('en-US').replace(/\//g, '-');;
  
    try {
      const jsonArray = array.map(item => {
        if (item[5] !== undefined) {
          item[5] = item[5].replace(/\[|\]/g, '');
          item[5] = item[5].replace(/(?<=\[|,)'|'(?=\]|,)/g, `''`);
        }
  
        if (item[6] !== undefined) {
          item[6] = item[6].replace(/\[|\]/g, '');
        }
  
        return item // Remove square brackets
      });
  
      //  console.log("Data inside upload: ", jsonArray)
  
      // Join the JSON strings with newlines
      const jsonString = jsonArray.join('\n');
  
      // console.log("json string: ", jsonString)
      // Create a buffer from the formatted JSON string
      const buffer = Buffer.from(jsonString, 'utf8');
  
      const params = {
        Bucket: process.env.AWS_BUCKET_NAME,
        Key: `result/${formattedDate}-${filename}`,
        Body: buffer,
      };
  
      var options = { partSize: 6 * 1024 * 1024, queueSize: 10 };
  
      const parallelUploads3 = new Upload({
        client: s3Client,
        queueSize: 4,
        leavePartsOnError: false,
        params: params,
        options
      });
  
      parallelUploads3.on("httpUploadProgress", (progress) => {
        console.log(progress);
      });
  
      await parallelUploads3.done();
    } catch (e) {
      console.log(e);
    }
  }
  
  export async function downloadCSVDataFromS3(folderName, fileType, skipFirstRow, allowHeaders) {
    try {
      const listObjectsParams = {
        Bucket: process.env.AWS_BUCKET_NAME,
        Prefix: `${folderName}`,
      };
  
      const { Contents } = await s3Client.send(new ListObjectsCommand(listObjectsParams));
  
      const filteredFilenames = Contents.filter((object) => object.Key.endsWith(`-${fileType}.csv`));
  
      filteredFilenames.sort((a, b) => b.LastModified - a.LastModified);
  
      if (filteredFilenames.length === 0) {
        console.log(`No ${fileType} files found in the specified folder.`);
        return [];
      }
  
      const newestFile = filteredFilenames[0];
      const { Key } = newestFile;
      //console.log("Failo pavadinimas: ", Key)
  
      const getObjectCommand = new GetObjectCommand({
        Bucket: process.env.AWS_BUCKET_NAME,
        Key,
      });
  
      const responseBody = await s3Client.send(getObjectCommand);
      const { Body } = responseBody;
  
      const data = [];
  
      // let acc = 0;
      await new Promise((resolve, reject) => {
        let acc = 0; // Initialize a row counter
        let lastRow; // Variable to store the last successfully parsed row
  
        fastcsv.parseStream(Body, { headers: allowHeaders })
          .on('data', (row) => {
            if (!skipFirstRow) {
              lastRow = row; // Store the last successfully parsed row
              data.push(row);
              acc++; // Increment the row counter
            } else {
              skipFirstRow = false;
            }
          })
          .on('end', () => {
            console.log(`Newest ${fileType} file parsed successfully. Line count ${acc}`);
            resolve();
          })
          .on('error', (error) => {
            console.error(`Error while parsing ${fileType} file at line ${acc}:`, error, lastRow);
            reject(error);
          });
      });
  
      return data;
    } catch (e) {
      console.error(`Error downloading and parsing ${fileType} file from S3:`, e);
      throw e;
    }
  }
