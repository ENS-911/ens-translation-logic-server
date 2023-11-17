const { Client } = require('pg');
const AWS = require('aws-sdk');

const databaseConfig = {
    user: 'your_db_user',
    host: 'your_db_host',
    database: 'your_db_name',
    password: 'your_db_password',
    port: 5432, // Change this according to your PostgreSQL setup
};