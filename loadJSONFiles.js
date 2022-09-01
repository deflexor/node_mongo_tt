import * as fs from 'fs';
import * as dotenv from 'dotenv';
import { exit } from 'process';

dotenv.config();

const colDetailsName = process.env.COLLECTION1_NAME
const colSummaryName = process.env.COLLECTION2_NAME

function idF(x) {
  return x;
}

function loadJSON(filename, filterTextF = idF, modifyJSONF = idF) {
  let json;
  try {
    const fileData = filterTextF(fs.readFileSync(filename).toString('utf8'));
    json = JSON.parse(fileData);
    modifyJSONF(json);
    return json;
  } catch (e) {
    console.error(`failed to load ${filename}! ${e}`);
    exit();
  } finally {
    return json;
  }
}

function fixSummaryJSON(text) {
  let r = text.replaceAll('country', '"country"');
  r = r.replaceAll("'", '"');
  return r;
}

const addIDsToJSON = (idFlds = []) => (json) => {
  if (idFlds.length === 0) return json;
  if (!Array.isArray(json)) return json;
  json.forEach((obj) => {
    if (!obj._id) {
      obj._id = idFlds.map((f) => obj[f] || 'x').join('_');
    }
  });
  return json;
};

export async function loadJSONFiles(db) {
  const detailsCollection = db.collection(colDetailsName);
  const summaryCollection = db.collection(colSummaryName);
  try {
    const detailsJson = loadJSON('./first.json', idF, addIDsToJSON(['country', 'city', 'name']));
    const summaryJson = loadJSON('./second.json', fixSummaryJSON, addIDsToJSON(['country']));
    await detailsCollection.insertMany(detailsJson);
    console.log('Loaded detailsJson');
    await summaryCollection.insertMany(summaryJson);
    console.log('Loaded summaryJson');
  } catch (e) {
    if (`${e}`.indexOf('E11000 duplicate key') !== -1) {
      // colleactions already loaded, ok to skip
    } else {
      console.error(`Error loading json into database; ${e}`);
      exit();
    }
  }
}
