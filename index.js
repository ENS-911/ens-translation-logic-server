const { Client } = require('pg');
const AWS = require('aws-sdk');
const dotenv = require('dotenv');
const { Pool } = require('pg');
dotenv.config();

const lambdaConfig = {
  region: process.env.AWS_REGION,
  accessKeyId: process.env.AWS_ACCESS_KEY_ID,
  secretAccessKey: process.env.AWS_SECRET_ACCESS_KEY,
};

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
  const pool = new Pool({
    user: 'ensclient',
    host: 'ens-client.cfzb4vlbttqg.us-east-2.rds.amazonaws.com',
    database: 'ens-client',
    password: 'gQ9Sf8cIczKhZiCswXXy',
    port: 5432,
  });


  const postgresClient = new Client(databaseConfig); // Create a new client instance for the main function

  try {
    const result = await pool.query('SELECT * FROM clients');
        res.json(result.rows);
    await Promise.all(result.rows.map(processDatabaseRow));
  } catch (error) {
    console.error('Error connecting to PostgreSQL:', error);
  } finally {
    await postgresClient.end();
  }
};

exports.handler = async (event, context) => {
  await main();
  return { statusCode: 200, body: 'Execution completed.' };
};