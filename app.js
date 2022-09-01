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
  const $coordsAgg = await db.collection(col1).aggregate([
    {
      $project: {
        country: 1,
        lng: { $arrayElemAt: ['$location.ll', 0] },
        lat: { $arrayElemAt: ['$location.ll', 1] },
      },
    },
    { $group: { _id: '$country', longitude: { $push: '$lng' }, latitude: { $push: '$lat' } } },
  ]).toArray();
  const coordsByCountry = new Map();
  $coordsAgg.forEach((row) => {
    coordsByCountry[row._id] = { latitude: row.latitude, longitude: row.longitude };
  });
  // Task part 4, students difference
  const $sdiffAgg = await db.collection(col1).aggregate([
    {
      $lookup:
      {
        from: col2,
        localField: 'country',
        foreignField: 'country',
        as: 'col2',
      },
    },
    { $project: { country: 1, students: 1, col2: 1 } },
    {
      $group:
       {
         _id: '$country',
         col2: { $first: '$col2.overallStudents' },
         detStudents: { $sum: { $sum: '$students.number' } },
       },
    },
    {
      $addFields: {
        diffStundents: { $subtract: ['$detStudents', { $first: '$col2' }] },
      },
    },
  ]).toArray();
  // Task part 5: Find documents count by countries
  const $docsAgg = await db.collection(col1).aggregate([
    { $group: { _id: '$country', count: { $sum: 1 } } },
    { $sort: { count: -1 } },
  ]).toArray();
  const countByCountry = new Map();
  $docsAgg.forEach((row) => {
    countByCountry[row._id] = row.count;
  });
  //  console.log($coordsAgg);
  // Task 6: Write result to third collection to the current database
  const result = $coordsAgg.map((row) => ({
    _id: row._id,
    allDiffs: $sdiffAgg,
    count: countByCountry[row._id],
    longitude: coordsByCountry[row._id].longitude,
    latitude: coordsByCountry[row._id].latitude,
  }));
  // console.log(result)
  await db.collection(col3).drop();
  await db.collection(col3).insertMany(result);
  return 'done.';
}

main()
  .then(console.log)
  .catch(console.error)
  .finally(() => client.close());
