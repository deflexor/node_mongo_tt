'use strict';
import * as fs from 'fs';
import { exit } from 'process';

export async function loadJSONFiles(db) {
  const detailsCollection = db.collection('details');
  const summaryCollection = db.collection('summary');
  try {
    const detailsJson = loadJSON("./first.json", idF, addIDsToJSON(['country', 'city', 'name']));
    const summaryJson = loadJSON("./second.json", fixSummaryJSON, addIDsToJSON(['country']));
    let insertResult = await detailsCollection.insertMany(detailsJson);
    console.log(`Loaded detailsJson`);
    insertResult = await summaryCollection.insertMany(summaryJson);
    console.log(`Loaded summaryJson`);
  } catch (e) {
    console.error(`Error loading json into database; ${e}`);
  }
}

export function idF(x) {
  return x
}

export function fixSummaryJSON(text) {
  let r = text.replaceAll('country', '"country"');
  r = r.replaceAll("'", '"');
  return r
}

export const addIDsToJSON = (idFlds = []) => (json) => {
  if(idFlds.length === 0) return json;
  if(!Array.isArray(json)) return json;
  for(let obj of json) {
    if(obj['_id']) continue;
    obj['_id'] = idFlds.map(f => obj[f] || 'x').join('_')
  }
  return json
}

export function loadJSON(filename, filterTextF = idF, modifyJSONF = idF) {
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