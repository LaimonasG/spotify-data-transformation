import fs from "fs";
import fastcsv from "fast-csv"

export function readCsvData(filePath, linesToRead, callback) {
  let linesRead = 0;
  const rowData = [];
  const header = []

  const stream = fs.createReadStream(filePath)
    .pipe(fastcsv.parse({ delimiter: ',', skipEmptyLines: true, headers: false }))
    .on('data', function (row) {
      // Skip header
      if (linesRead === 0) {
        header.push(row)
        linesRead++;
        return;
      }

      rowData.push(row);
      linesRead++;

      if (linesRead === linesToRead && linesToRead !== null) {
        stream.destroy();
        callback(null, rowData, header);
      }
    })
    .on('end', function () {
      console.log('Finished reading file.');
      callback(null, rowData, header);
    })
    .on('error', function (error) {
      console.error('Error:', error.message);
      callback(error, null);
    });
}

export function writeCsvData(outputFilePath, data, callback) {
  const ws = fs.createWriteStream(outputFilePath);

  fastcsv
    .write(data, { headers: false, delimiter: ',' })
    .on('finish', () => {
      callback(null);
    })
    .on('error', (error) => {
      console.error('Error:', error.message);
      callback(error);
    })
    .pipe(ws);
}

export function readFileToBuffer(filePath, callback) {
  let linesRead = 0;
  const rowData = [];
  const header = [];

  fs.createReadStream(filePath)
    .pipe(fastcsv.parse({ delimiter: ',', skipEmptyLines: true, headers: false }))
    .on('data', function (row) {
      // Skip header
      if (linesRead === 0) {
        header.push(row);
        linesRead++;
        return;
      }

      rowData.push(row);
      linesRead++;

    })
    .on('end', function () {
      console.log('Finished reading file.');
      const csvData = rowData.map((row) => row.join(',')).join('\n'); // Convert data to CSV format
      const buffer = Buffer.from(header.map((row) => row.join(',')).join('\n') + '\n' + csvData); // Create a buffer

      callback(null, buffer);
    })
    .on('error', function (error) {
      console.error('Error:', error.message);
      callback(error, null);
    });
}
