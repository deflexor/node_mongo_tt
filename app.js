'use strict';
import * as dotenv from 'dotenv'
dotenv.config()
import { MongoClient } from 'mongodb'
import * as fs from 'fs';
import { exit } from 'process';


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
  const detailsCollection = db.collection('details');
  const summaryCollection = db.collection('summary');
  try {
    const detailsJson = loadJSON("./first.json", idF, addIDsToJSON(['country','city','name']));
    const summaryJson = loadJSON("./second.json", fixSummaryJSON, addIDsToJSON(['country']));
    let insertResult = await detailsCollection.insertMany(detailsJson);
    console.log(`Loaded detailsJson`);
    insertResult = await summaryCollection.insertMany(summaryJson);
    console.log(`Loaded summaryJson`);
  } catch (e) {
    console.error(`Error loading json into database; ${e}`)
  }
  // the following code examples can be pasted here...

  return 'done.';
}

function idF(x) {
  return x
}

function fixSummaryJSON(text) {
  let r = text.replaceAll('country', '"country"');
  r = r.replaceAll("'", '"');
  return r
}

const addIDsToJSON = (idFlds = []) => (json) => {
  if(idFlds.length === 0) return json;
  if(!Array.isArray(json)) return json;
  for(let obj of json) {
    if(obj['_id']) continue;
    obj['_id'] = idFlds.map(f => obj[f] || 'x').join('_')
  }
  return json
}

function loadJSON(filename, filterTextF = idF, modifyJSONF = idF) {
  try {
    const fileData = filterTextF(fs.readFileSync(filename).toString('utf8'));
    const json = JSON.parse(fileData);
    modifyJSONF(json);
    return json;
  } catch (e) {
    console.error(`failed to load ${filename}! ${e}`);
    process.exit();
  }
}

main()
  .then(console.log)
  .catch(console.error)
  .finally(() => client.close());