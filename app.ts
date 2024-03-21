import * as fs from 'fs';
import * as readline from 'readline';
import * as mysql from 'mysql';

// Create MySQL connection pool
const pool = mysql.createPool({
  connectionLimit: 10,
  host: 'localhost',
  user: 'username',
  password: 'password',
  database: 'database',
});

// Function to parse 200 record
function parse200Record(record: string): [string, number] {
  const parts = record.split(',');
  const nmi = parts[2];
  const intervalLength = parseInt(parts[8], 10);
  return [nmi, intervalLength];
}

// Function to parse 300 record
function parse300Record(record: string): [Date, number[]] {
  const parts = record.split(',');
  const intervalDate = new Date(parts[1].substring(0, 4), parts[1].substring(4, 6), parts[1].substring(6, 8));
  const consumptionValues = parts.slice(14).map(part => parseFloat(part));
  return [intervalDate, consumptionValues];
}

// Function to generate SQL insert statements
function generateInsertStatements(nmi: string, intervalLength: number, intervalDate: Date, consumptionValues: number[]): string[] {
  const sqlInserts: string[] = [];
  for (let i = 0; i < consumptionValues.length; i++) {
    const timestamp = new Date(intervalDate.getTime() + i * 24 * 60 * 60 * 1000);
    const sql = `INSERT INTO meter_readings ("nmi", "timestamp", "consumption") VALUES ('${nmi}', '${timestamp.toISOString()}', ${consumptionValues[i]});`;
    sqlInserts.push(sql);
  }
  return sqlInserts;
}

// Function to process insertions asynchronously
async function processInsertions(sqlStatements: string[]) {
  for (const sql of sqlStatements) {
    await new Promise<void>((resolve, reject) => {
      pool.query(sql, (error, results, fields) => {
        if (error) {
          console.error('Error inserting data:', error);
          reject(error);
        } else {
          console.log('Inserted successfully:', sql);
          resolve();
        }
      });
    });
  }
}

// Read and process the input file using streams
function processInputFile(inputFilePath: string) {
  const fileStream = fs.createReadStream(inputFilePath);
  const rl = readline.createInterface({
    input: fileStream,
    crlfDelay: Infinity,
  });

  let nmi: string | undefined;
  let intervalLength: number | undefined;
  let currentRecord: string[] | undefined;
  let sqlStatements: string[] = [];

  rl.on('line', (line: string) => {
    const record = line.split(',');
    if (record[0] === '200') {
      [nmi, intervalLength] = parse200Record(line);
    } else if (record[0] === '300' && nmi && intervalLength) {
      currentRecord = record;
    } else if (currentRecord && record[0] === '500') {
      const [intervalDate, consumptionValues] = parse300Record(currentRecord.join(','));
      const insertStatements = generateInsertStatements(nmi!, intervalLength!, intervalDate, consumptionValues);
      sqlStatements = sqlStatements.concat(insertStatements);
      currentRecord = undefined;
    }
  });

  rl.on('close', async () => {
    try {
      await processInsertions(sqlStatements);
      console.log('All insertions completed.');
    } catch (error) {
      console.error('Error processing insertions:', error);
    } finally {
      pool.end();
    }
  });
}

// Call the function to process the input file
processInputFile('input_file.csv');
