const { Client } = require('pg');
const AWS = require('aws-sdk');
const dotenv = require('dotenv');
const { Pool } = require('pg');
dotenv.config();

const lambdaConfig = {
  region: process.env.AWS_REGION,
};

const lambda = new AWS.Lambda(lambdaConfig);

const databaseTypeToLambdaMap = {
  'mssql': 'lambda_translator_mssql',
  'mysql': 'lambda_translator_mysql',
  'pg': 'lambda_translator_pg',
};

async function processDatabaseRow(row) {

  const { db_type, dbsync } = row;

  if (dbsync.toLowerCase() === 'active' && databaseTypeToLambdaMap.hasOwnProperty(db_type)) {
    const lambdaFunctionName = databaseTypeToLambdaMap[db_type];
    
    try {
      const staticPayload = {
    "id": "1",
    "key": "9c44f764-fbf7-424c-820b-b954a7899acd",
    "name": "Hamilton County 9-1-1",
    "address": "3404 Amnicola Hwy",
    "city": "Chattanooga",
    "state": "TN",
    "zip": "37406",
    "plan": "silver",
    "db_type": "mssql",
    "agency_type": "agency_type",
    "battalion": "battalion",
    "db_city": "city",
    "creation": "creation",
    "crossstreets": "crossstreets",
    "entered_queue": "entered_queue",
    "db_id": "id",
    "jurisdiction": "jurisdiction",
    "latitude": "latitude",
    "location": "location",
    "longitude": "longitude",
    "master_incident_id": "master_incident_id",
    "premise": "premise",
    "priority": "priority",
    "sequencenumber": "sequencenumber",
    "stacked": "stacked",
    "db_state": "state",
    "status": "status",
    "statusdatetime": "statusdatetime",
    "type": "type",
    "type_description": "type_description",
    "zone": "zone",
    "raw_server": "database-911.cfzb4vlbttqg.us-east-2.rds.amazonaws.com",
    "raw_user": "admin365",
    "raw_pass": "MzmEG21PQSMDW4qXPsQF",
    "trans_db_loc": "XX",
    "raw_table": "hc911_db",
    "phone_number": "423-622-1911",
    "email": "info_911@hc911.org",
    "website": "hc911.org",
    "active": "inactive",
    "dbsync": "active",
    "raw_table_name": "active_incidents"
};
      // Ensure row is a valid JSON before sending it
      const payload = JSON.parse(staticPayload);
      const rowPayload = JSON.stringify(payload);
      //const rowPayload = staticPayload;

      const lambdaParams = {
        FunctionName: lambdaFunctionName,
        InvocationType: 'RequestResponse',
        Payload  : rowPayload,
      };

      //console.log('Lambda Parameters:', lambdaParams);
      console.log('Row Payload:', rowPayload);

      const lambdaResponse = await lambda.invoke(lambdaParams).promise();
      console.log('Lambda Response:', lambdaResponse);
    } catch (error) {
      console.error('Error processing row:', error);
      // Log the error and continue with the next row
    }
  }
}

async function main() {
  console.log('main function called')
  const pool = new Pool({
    user: 'ensclient',
    host: 'ens-client.cfzb4vlbttqg.us-east-2.rds.amazonaws.com',
    database: 'postgres',
    password: 'gQ9Sf8cIczKhZiCswXXy',
    port: 5432,
    max: 20,
    ssl: {
      rejectUnauthorized: false, // Ignore unauthorized SSL errors (not recommended for production)
    },
  });

  try {
    const result = await pool.query('SELECT * FROM public.clients');
    console.log(result)
    await Promise.all(result.rows.map(processDatabaseRow));
  } catch (error) {
    console.error('Error connecting to PostgreSQL:', error);
    // Rethrow the error or return a meaningful response
    throw error;
  } finally {
    // Release the pool when done
    pool.end();
  }
}

module.exports.handler = async (event, context) => {
  try {
    await main();
    return { statusCode: 200, body: 'Execution completed.' };
  } catch (error) {
    console.error('Lambda execution error:', error);
    return { statusCode: 500, body: 'Internal Server Error.' };
  }
};