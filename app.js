'use strict';
import * as dotenv from 'dotenv'
dotenv.config()
import { MongoClient } from 'mongodb'
import { loadJSONFiles } from './loadJSONFiles.js';


// Connection URL
const url = process.env.DB_URL;
const client = new MongoClient(url);

// Database Name
const dbName = process.env.DB_NAME;

async function main() {
  // Use connect method to connect to the server
  await client.connect();
  console.log('Connected successfully to server');
  const db = client.db(dbName);
  await loadJSONFiles(db);
  // the following code examples can be pasted here...

  return 'done.';
}



main()
  .then(console.log)
  .catch(console.error)
  .finally(() => client.close());