const AWS = require('aws-sdk');
const { Pool } = require('pg');
const dotenv = require('dotenv');
dotenv.config();

const lambda = new AWS.Lambda({ region: process.env.AWS_REGION });

async function processAndInvokeSecondLambda(data) {
  console.log('Processing row data:', data);

  // Prepare payload to send to the second Lambda
  const rowPayload = JSON.stringify(data);

  console.log('Payload being sent to second Lambda:', rowPayload);

  const lambdaParams = {
    FunctionName: 'lambda_translator_mssql',  // Second Lambda function
    InvocationType: 'RequestResponse',        // Can be 'Event' for async invocation
    Payload: rowPayload,                      // Pass the fetched data as a payload
  };

  try {
    console.log('Invoking Second Lambda with:', lambdaParams);
    const lambdaResponse = await lambda.invoke(lambdaParams).promise();
    console.log('Second Lambda Response:', lambdaResponse);

    if (lambdaResponse.Payload) {
      const parsedPayload = JSON.parse(lambdaResponse.Payload);
      console.log('Parsed Payload from Second Lambda:', parsedPayload);
    }
  } catch (error) {
    console.error('Error invoking second Lambda:', error);
  }
}

async function main(event) {
  console.log('Received Scheduled Event:', JSON.stringify(event, null, 2));

  // PostgreSQL connection details
  const pool = new Pool({
    user: 'ensclient',
    host: 'ens-client-v2.cfzb4vlbttqg.us-east-2.rds.amazonaws.com',
    database: 'postgres',
    password: 'gQ9Sf8cIczKhZiCswXXy',
    port: 5432,
    max: 20,
    ssl: {
      rejectUnauthorized: false,
    },
  });

  try {
    // Query PostgreSQL for the data you need to process
    const result = await pool.query('SELECT * FROM public.clients');  // Replace with your actual query
    console.log('Fetched data from PostgreSQL:', result.rows);

    // Process each row and invoke the second Lambda
    for (const row of result.rows) {
      await processAndInvokeSecondLambda(row);
    }

  } catch (error) {
    console.error('Error fetching data from PostgreSQL:', error);
  } finally {
    await pool.end();
  }
}

// AWS Lambda Handler
module.exports.handler = async (event) => {
  try {
    await main(event);
    return { statusCode: 200, body: 'First Lambda executed successfully' };
  } catch (error) {
    console.error('First Lambda execution error:', error);
    return { statusCode: 500, body: 'Internal Server Error' };
  }
};
