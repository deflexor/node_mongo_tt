import * as dotenv from 'dotenv';
import { MongoClient } from 'mongodb';
import { loadJSONFiles } from './loadJSONFiles.js';

dotenv.config();

// Connection Data
const url = process.env.DB_URL;
const client = new MongoClient(url);
const dbName = process.env.DB_NAME;
const col1 = process.env.COLLECTION1_NAME;
const col2 = process.env.COLLECTION2_NAME;
const col3 = 'result';

async function main() {
  await client.connect();
  console.log('Connected successfully to server');
  const db = client.db(dbName);
  await loadJSONFiles(db);
  // Task part 3, aggregate coords
  const $agg = await db.collection(col1).aggregate([
    {
      $lookup:
      {
        from: col2,
        localField: 'country',
        foreignField: 'country',
        as: 'col2',
      },
    },    
    {
      $project: {
        country: 1,
        lng: { $arrayElemAt: ['$location.ll', 0] },
        lat: { $arrayElemAt: ['$location.ll', 1] },
        students: 1,
        col2: 1,
      },
    },
    {
      $group:
       {
         _id: '$country',
         col2: { $first: '$col2.overallStudents' },
         detStudents: { $sum: { $sum: '$students.number' } },
         longitude: { $push: '$lng' },
         latitude: { $push: '$lat' },
         count: { $sum: 1 }
       },
    },
    {
      $addFields: {
        diffStundents: { $subtract: ['$detStudents', { $first: '$col2' }] },
      },
    },
    {
      $project: {
        country: 1,
        longitude: 1,
        latitude: 1,
        count: 1,
        diffStundents: 1,
      },
    },
  ]).toArray();

  await db.collection(col3).drop();
  await db.collection(col3).insertMany($agg);
  return 'done.';
}

main()
  .then(console.log)
  .catch(console.error)
  .finally(() => client.close());
