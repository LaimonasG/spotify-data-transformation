# Spotify data transformation  

This task includes ingestion, transformation and analysis of Spotify datasets. Implemented using node.js with "AWS S3 Bucket", "AWS Aurora" (Posgresql) integrations.  

## Table of contents 

- [Getting Started](#getting-started)  
- [Prerequisites](#prerequisites)  
- [Installation](#installation)  
- [Usage](#usage)  
- [Differences from the Task](#differences-from-the-task)  

## Getting started 

The main fil of this project is index.js. All helper methods and sample data are fragmented inside dedicated folders. 
Objectives and the workflow of the task are presented below: 

### Data source 
https://www.kaggle.com/datasets/yamaerenay/spotify-dataset-19212020-600k-tracks?select=artists.csv; 
https://www.kaggle.com/datasets/yamaerenay/spotify-dataset-19212020-600k-tracks?select=tracks.csv. 

## Data Transformation: 
Implement a Node.js script to ingest the data from the source and transform it as follows: 
 -Filter out records that meet specific criteria: 
 -Ignore the tracks that have no name; 
 -Ignore the tracks shorter than 1 minute; 
 -Load only these artists that have tracks after the filtering above. 
 
Format and structure the data as needed for analysis:  
 -Explode track release date into separate columns: year, month, day. 
 -Transform track danceability into string values based on these intervals: 
 [0; 0.5) assign ‘Low’;  <br>
 [0,5; 0.6] assign ‘Medium’;   <br>
 (0.6; 1] assign ‘High’.   <br>
 
## Data Storage: 
 -Store the cleaned and transformed data into AWS S3 as your storage solution. 
 -Load data from S3 into locally hosted PostgreSQL. Bonus if you load the data into AWS Aurora instead. 

## Data Processing: 
Create SQL views that perform the following tasks on the data stored: 
 -Take track: id, name, popularity, energy, danceability (Low, Medium, High); and number of artist followers; 
 -Take only these tracks, which artists has followers; 
 -Pick the most energising track of each release year. 


## Prerequisites 

Before you begin, ensure you have met the following requirements: 

- **Node.js**: This project requires Node.js to be installed. You can download it from [nodejs.org](https://nodejs.org/). 

- **Node.js Packages**: You will need the following Node.js packages, which will be installed automatically when you run `npm install`: 

  - `@aws-sdk/client-s3`: AWS SDK for Amazon S3 
  - `@aws-sdk/lib-storage`: AWS SDK for storage services (used by `@aws-sdk/client-s3`) 
  - `aws-sdk`: AWS SDK for general AWS services 
  - `dotenv`: Environment variable management 
  - `fast-csv`: CSV parsing and formatting library 
  - `pg`: PostgreSQL client for Node.js 

## Installation

Follow these steps to get the project up and running on your local machine. 

```bash
   git clone https://github.com/LaimonasG/spotify-data-transformation.git 
   cd spotify-data-transformation
   npm install
```

After this, you will need to add enviromental variables to .env file in root directory: 

AWS_BUCKET_NAME   <br>
AWS_ACCESS_KEY_ID   <br>
AWS_SECRET_ACCESS_KEY   <br>
S3_REGION   <br>

AWS_AURORA_ENDPOINT  <br>
AWS_AURORA_PORT   <br>
AWS_AURORA_USERNAME   <br>
AWS_AURORA_PASSWORD   <br>
AWS_AURORA_DATABASE   <br>

The dowloaded dataset files need to be placed inside data/ folder, in root directory. <br>
File names are specified by dedicated variables, in index.js.

Due to this being a public repository, i can't share AWS configuration.

## Usage 

You can run the app using the following command: 

```bash
   node index.js
```

Then in the console you will see information about the read data, transformed data, uploaded data and finally the sql view results will be displayed. 
Using variables linesToRead and insertBatchSize, inside index.js file, you can change the amount of rows read and the batch size for uploading the data to 
Aurora RDS. If linesToRead value is set to null, all rows will be read. Also, the myData/ folder contains some sample data in smaller quantities for testing.

Uploaded S3 bucket data is stored inside result/ folder, each name has a unique timestamp: 

![image](https://github.com/LaimonasG/spotify-data-transformation/assets/79421767/0ca5f2e9-05ce-47ce-8623-6d1abf06e3b5)


## Differences from the task 

The finished task has some parts unfinished, due to lack of expertise with big data. 

 -I was able to upload all the data in parralel to S3 Bucket, but couldn't download it. Spent a considerable amount of time, trying to "clean up" the data, 
 using regex replace statements. Parsing fails due to track names, because they have unexpected symbols for the parser.

 -Uploading to Aurora RDS is also flawed. Current approach mimics batch insertion, but still performs seperate insert statements for each row. Explored <br>
 posibilities to insert using COPY statement, this is probably the correct approach, but I didn't have enough time. <br>

 -First view logic seems fine, but it keeps returning default 0 value for followers_sum. Because the sum logic is flawed, the second view also doesn't
 return proper result. Tried using left joins and lateral joins. Third view seems to work as intended.

 
