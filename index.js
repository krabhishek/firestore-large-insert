const { BigQuery } = require('@google-cloud/bigquery');
const admin = require('firebase-admin');
const serviceAccount = require('./project-cred.json');

admin.initializeApp({
  credential: admin.credential.cert(serviceAccount)
});

var db = admin.firestore();
const BATCH_SIZE = 250;

async function Bq(limit) {

// A simple function to pull sample data from a public dataset from BQ.
// Write your own data source function here.
  // Creates a client
  const bigquery = new BigQuery();

  const query = `SELECT zipcode, geo_id, population FROM \`bigquery-public-data:census_bureau_usa.population_by_zip_2010\`
  LIMIT ${limit}`;
  const options = {
    query: query,
    // Location must match that of the dataset(s) referenced in the query.
    location: 'US'
  };
  
  // Runs the query as a job
  const [job] = await bigquery.createQueryJob(options);
  console.log(`Job ${job.id} started.`);

  // Waits for the query to finish
  const [rows] = await job.getQueryResults();

  // Prints the results
  console.log('Rows pulled from BQ:', rows.length);
  return rows;
}


async function writeWithBatch(count) {
  let data = [];
  let bref = [];
  data = await Bq(count);
  let mul = 0;
  let b = db.batch();
  console.log('--- Starting Batch ----- ', Date.now());
  for (let i = 0; i < data.length; i++) {
    mul = i;
    if (mul % BATCH_SIZE == 0) {
      x = Date.now();
      console.log(`>> STARTTING BATCH COMMIT - TIMESTAMP   ${Date.now()}`);
      await b.commit().catch(err => console.log(`XXXX ERROR XXXX`, err));
      y = Date.now();
      console.log(
        `>>> [${i}] MADE COMMIT  - COMPLETE TIMESTAMP ${Date.now()}   --- TIME TAKEN  : ${y -
          x}`
      );
      //await setTimeout(() => console.log('#####'), 1000);
      mul = 0;
      b = db.batch();
    } else {
      b.set(db.collection('ltfsnonbatch').doc(), data[i]);
    }
  }
  await b.commit().then(() => {
    console.log('------ MADE FINAL COMMIT ----------');
    console.log(Date.now());
  });
}

writeWithBatch(500000); // Pull 500,000 records from BQ and insert into Firestore
