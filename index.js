const { Client } = require('pg');
const AWS = require('aws-sdk');
const dotenv = require('dotenv');
dotenv.config();

const databaseConfig = {
  user: process.env.DB_USER,
  host: process.env.DB_HOST,
  database: process.env.DB_NAME,
  password: process.env.DB_PASSWORD,
  port: process.env.DB_PORT,
  max: 10, // Adjust as needed for connection pooling
};

const lambdaConfig = {
  region: process.env.AWS_REGION,
  accessKeyId: process.env.AWS_ACCESS_KEY_ID,
  secretAccessKey: process.env.AWS_SECRET_ACCESS_KEY,
};

const postgresClient = new Client(databaseConfig);
const lambda = new AWS.Lambda(lambdaConfig);

const databaseTypeToLambdaMap = {
  'mssql': 'lambda_translator_mssql',
  'mysql': 'lambda_translator_mysql',
  'pg': 'lambda_translator_pg',
};

async function processDatabaseRow(row) {
  const { db_type, dbsync } = row;

  if (dbsync.toLowerCase() === 'yes' && databaseTypeToLambdaMap.hasOwnProperty(db_type)) {
    const lambdaFunctionName = databaseTypeToLambdaMap[db_type];
    const lambdaParams = {
      FunctionName: lambdaFunctionName,
      InvocationType: 'RequestResponse',
      Payload: JSON.stringify(row),
    };

    try {
      const lambdaResponse = await lambda.invoke(lambdaParams).promise();
      console.log('Lambda Response:', lambdaResponse);
    } catch (error) {
      console.error('Error invoking Lambda function:', error);
      // Log the error and continue with the next row
    }
  }
}

async function main() {
  try {
    await postgresClient.connect();

    const queryResult = await postgresClient.query('SELECT * FROM clients');
    await Promise.all(queryResult.rows.map(processDatabaseRow));
  } catch (error) {
    console.error('Error connecting to PostgreSQL:', error);
  } finally {
    await postgresClient.end();
  }
}

exports.handler = async (event, context) => {
  await main();
  return { statusCode: 200, body: 'Execution completed.' };
};