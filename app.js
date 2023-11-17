const { Client } = require('pg');
const AWS = require('aws-sdk');
const dotenv = require('dotenv');

const databaseConfig = {
    user: process.env.DB_USER,
    host: process.env.DB_HOST,
    database: process.env.DB_NAME,
    password: process.env.DB_PASSWORD,
    port: process.env.DB_PORT,
};
  
const lambdaConfig = {
    region: process.env.AWS_REGION,
    accessKeyId: process.env.AWS_ACCESS_KEY_ID,
    secretAccessKey: process.env.AWS_SECRET_ACCESS_KEY,
};
  
const postgresClient = new Client(databaseConfig);
const lambda = new AWS.Lambda(lambdaConfig);