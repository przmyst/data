// densityWorker.js
const fs = require('fs');
const fsPromises = require('fs').promises;
const path = require('path');
const os = require('os');
const Papa = require('papaparse');
const shapefile = require('shapefile');
const unzipper = require('unzipper');
const h3 = require('h3-js');
const turf = require('@turf/turf');
const { fork } = require('child_process');

// === Logging Helpers ===
function logInfo(message) {
  console.log(`[INFO] ${new Date().toISOString()} - ${message}`);
}
function logError(message, error) {
  if (error) {
    console.error(`[ERROR] ${new Date().toISOString()} - ${message}`, error);
  } else {
    console.error(`[ERROR] ${new Date().toISOString()} - ${message}`);
  }
}

// Use the RESOLUTION from environment (default to 7 if not provided)
const resolution = process.env.RESOLUTION ? parseInt(process.env.RESOLUTION) : 7;

async function extractZip(zipFilePath) {
  logInfo(`Extracting ZIP file: ${zipFilePath}`);
  const tempDir = await fsPromises.mkdtemp(path.join(os.tmpdir(), 'shapefile-'));
  await fs.createReadStream(zipFilePath)
    .pipe(unzipper.Extract({ path: tempDir }))
    .promise();
  logInfo(`Extracted to temporary directory: ${tempDir}`);
  return tempDir;
}

function chunkArray(array, chunks) {
  const chunkSize = Math.ceil(array.length / chunks);
  const result = [];
  for (let i = 0; i < array.length; i += chunkSize) {
    result.push(array.slice(i, i + chunkSize));
  }
  return result;
}

async function processFips(fips) {
  logInfo(`\n=== Starting processing for FIPS: ${fips} at resolution ${resolution} ===`);

  // === Check if file has already been processed ===
  const outputDir = path.join(__dirname, 'density', `${fips}/${resolution}`);
  const outputFilePath = path.join(outputDir, `${fips}.json`);
  if (fs.existsSync(outputFilePath)) {
    logInfo(`Hexagon density data for FIPS ${fips} at resolution ${resolution} already exists at ${outputFilePath}. Skipping processing.`);
    return;
  }

  // === 1. Load Census Tract Population Data ===
  logInfo('Loading census CSV data...');
  const csvFilePath = `./census/density/${fips}.csv`;
  if (!fs.existsSync(csvFilePath)) {
    logError(`CSV file for FIPS ${fips} not found at ${csvFilePath}`);
    return;
  }
  let csvContent;
  try {
    csvContent = fs.readFileSync(csvFilePath, 'utf8');
    logInfo(`Successfully read CSV file for FIPS ${fips}`);
  } catch (err) {
    logError(`Error reading CSV file for FIPS ${fips} at ${csvFilePath}`, err);
    return;
  }

  const parsedCSV = Papa.parse(csvContent, { header: true });
  logInfo(`Parsed CSV: ${parsedCSV.data.length} rows found.`);
  const tractData = {};
  parsedCSV.data.forEach(row => {
    const paddedTract = row.TRACT.toString().padStart(6, '0');
    const population = parseFloat(row.POP100);
    const areaLand = parseFloat(row.AREALAND); // assuming area in m²
    if (areaLand > 0) {
      const density = (population / areaLand) * 1e6;
      tractData[paddedTract] = { density, population, areaLand };
    }
  });
  logInfo(`Loaded population and density data for ${Object.keys(tractData).length} census tracts.`);

  // === 2. Load Census Tract Shapefile ===
  logInfo('Extracting and loading census shapefile...');
  const zipFilePath = `./tracts/2020/tl_2020_${fips}_tract.zip`;
  if (!fs.existsSync(zipFilePath)) {
    logError(`Shapefile ZIP for FIPS ${fips} not found at ${zipFilePath}`);
    return;
  }
  let tempDir;
  try {
    tempDir = await extractZip(zipFilePath);
  } catch (err) {
    logError(`Error extracting ZIP file for FIPS ${fips}`, err);
    return;
  }
  const shpFilePath = path.join(tempDir, `tl_2020_${fips}_tract.shp`);
  const dbfFilePath = path.join(tempDir, `tl_2020_${fips}_tract.dbf`);

  const tractGeojson = { type: "FeatureCollection", features: [] };
  try {
    const source = await shapefile.open(shpFilePath, dbfFilePath);
    let featureCount = 0;
    while (true) {
      const result = await source.read();
      if (result.done) break;
      tractGeojson.features.push(result.value);
      featureCount++;
    }
    logInfo(`Shapefile read successfully: ${featureCount} features loaded.`);
  } catch (error) {
    logError('Error reading shapefile', error);
    return;
  }

  // Attach density property to each tract using the last 6 characters of GEOID.
  tractGeojson.features.forEach(feature => {
    const geoid = feature.properties.GEOID;
    const tractKey = geoid.slice(-6);
    if (tractData[tractKey]) {
      feature.properties.DENSITY = tractData[tractKey].density;
      feature.properties.POPULATION = tractData[tractKey].population;
      feature.properties.AREALAND = tractData[tractKey].areaLand;
    }
  });
  logInfo(`Attached density and population properties to shapefile features.`);

  // === 3. Define State Boundary and Compute Hexagons ===
  logInfo('Loading state boundary GeoJSON...');
  const stateGeoJSONPath = path.join(__dirname, 'states', `${fips}.geojson`);
  let stateGeoJSON;
  try {
    const stateContent = fs.readFileSync(stateGeoJSONPath, 'utf8');
    stateGeoJSON = JSON.parse(stateContent);
    logInfo(`State boundary GeoJSON loaded from ${stateGeoJSONPath}`);
  } catch (error) {
    logError('Error reading state boundary GeoJSON', error);
    return;
  }

  let stateBoundary = [];
  if (stateGeoJSON.type === 'FeatureCollection') {
    const feature = stateGeoJSON.features[0];
    if (feature.geometry.type === 'Polygon') {
      stateBoundary = feature.geometry.coordinates[0]; // outer ring
    } else if (feature.geometry.type === 'MultiPolygon') {
      stateBoundary = feature.geometry.coordinates[0][0]; // first polygon's outer ring
    } else {
      logError('Unsupported geometry type:', feature.geometry.type);
      return;
    }
  } else if (stateGeoJSON.type === 'Feature') {
    if (stateGeoJSON.geometry.type === 'Polygon') {
      stateBoundary = stateGeoJSON.geometry.coordinates[0];
    } else if (stateGeoJSON.geometry.type === 'MultiPolygon') {
      stateBoundary = stateGeoJSON.geometry.coordinates[0][0];
    } else {
      logError('Unsupported geometry type:', stateGeoJSON.geometry.type);
      return;
    }
  } else {
    logError('Invalid GeoJSON format for state boundary.');
    return;
  }
  logInfo(`State boundary extracted with ${stateBoundary.length} coordinates.`);

  // h3-js expects polygon coordinates in [lat, lng] order.
  const stateBoundaryLatLng = stateBoundary.map(coord => [coord[1], coord[0]]);
  const polygon = [stateBoundaryLatLng];

  logInfo(`Computing hexagons for state ${fips} at resolution ${resolution}...`);
  const hexagons = h3.polygonToCells(polygon, resolution);
  logInfo(`Computed ${hexagons.length} hexagons for state ${fips}.`);

  // === 4. Compute Density for Each Hexagon Concurrently via Clustered Child Processes ===
  logInfo('Beginning concurrent hexagon density computation using child processes...');

  // Write the heavy tractGeojson to a temporary file so that sub-processes can load it without receiving it over IPC.
  const tractTempFile = path.join(os.tmpdir(), `tractGeojson_${fips}_${Date.now()}.json`);
  fs.writeFileSync(tractTempFile, JSON.stringify(tractGeojson));
  logInfo(`Wrote tract GeoJSON to temporary file: ${tractTempFile}`);

  // Determine number of sub-processes (e.g. one per available CPU minus one).
  const subProcessCount = Math.max(os.cpus().length - 1, 1);
  const hexChunks = chunkArray(hexagons, subProcessCount);

  // For each chunk, fork a new process running hexClusterWorker.js.
  const childPromises = hexChunks.map(chunk => {
    return new Promise((resolve, reject) => {
      const child = fork(path.join(__dirname, 'hexClusterWorker.js'), [], {
        env: {
          // Pass the hexagon chunk as a JSON string.
          HEX_CHUNK: JSON.stringify(chunk),
          // Pass the temporary file path for the tractGeojson.
          TRACT_FILE: tractTempFile
        },
        stdio: ['inherit', 'inherit', 'inherit', 'ipc']
      });

      child.on('message', (result) => {
        if (result && result.error) {
          reject(new Error(result.error));
        } else {
          resolve(result);
        }
      });
      child.on('error', reject);
      child.on('exit', code => {
        if (code !== 0) {
          reject(new Error(`Child process exited with code ${code}`));
        }
      });
    });
  });

  let hexResults;
  try {
    const resultsArray = await Promise.all(childPromises);
    // Each child returns an array of results; flatten them.
    hexResults = resultsArray.flat();
  } catch (err) {
    logError('Error during concurrent hexagon density computation', err);
    return;
  }

  // Remove the temporary tract GeoJSON file.
  try {
    fs.unlinkSync(tractTempFile);
    logInfo(`Removed temporary file: ${tractTempFile}`);
  } catch (err) {
    logError(`Error removing temporary file: ${tractTempFile}`, err);
  }

  // Organize results into an object keyed by hex.
  const hexData = {};
  hexResults.forEach(result => {
    hexData[result.hex] = result;
  });

  logInfo('Completed concurrent hexagon density computation.');

  // === 5. Save Hexagon Density Data to a JSON File ===
  try {
    if (!fs.existsSync(outputDir)) {
      fs.mkdirSync(outputDir, { recursive: true });
      logInfo(`Created output directory: ${outputDir}`);
    }
    fs.writeFileSync(outputFilePath, JSON.stringify(hexData, null, 2));
    logInfo(`Successfully stored hexagon density data for ${fips} at resolution ${resolution} to ${outputFilePath}`);
  } catch (err) {
    logError('Error saving hexagon density data to file', err);
  }
}

module.exports = processFips;
