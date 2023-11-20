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
};

const lambdaConfig = {
  region: process.env.AWS_REGION,
  accessKeyId: process.env.AWS_ACCESS_KEY_ID,
  secretAccessKey: process.env.AWS_SECRET_ACCESS_KEY,
};

const postgresClient = new Client(databaseConfig);
const lambda = new AWS.Lambda(lambdaConfig);

const databaseTypeToLambdaMap = {
  'mssql': 'lambda_function_name_1',
  'mysql': 'lambda_function_name_2',
  'pg': 'lambda_function_name_3',
  // Add more types as needed
};

async function processDatabaseRow(row) {
  const databaseType = row.db_type;
  const dbSync = row.dbsync;

  if (dbSync.toLowerCase() === 'yes') {
    if (databaseTypeToLambdaMap.hasOwnProperty(databaseType)) {
      const lambdaFunctionName = databaseTypeToLambdaMap[databaseType];

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
      }
    } else {
      console.error(`No Lambda function defined for database type: ${databaseType}`);
    }
  } else {
    console.log(`Bypassing row due to dbsync value: ${dbSync}`);
  }
}

async function main() {
  try {
    await postgresClient.connect();

    const queryResult = await postgresClient.query('SELECT * FROM your_table');

    for (const row of queryResult.rows) {
      await processDatabaseRow(row);
    }
  } catch (error) {
    console.error('Error connecting to PostgreSQL:', error);
  } finally {
    await postgresClient.end();
  }
}

main();