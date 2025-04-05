// hexClusterWorker.js
const fs = require('fs');
const path = require('path');
const h3 = require('h3-js');
const turf = require('@turf/turf');

function computeHexDensity(hex, tractGeojson) {
  let boundary = h3.cellToBoundary(hex, true);
  if (boundary.length > 0) boundary.push(boundary[0]);
  const hexPolygon = turf.polygon([boundary]);

  let totalPopulation = 0;
  const hexAreaM2 = turf.area(hexPolygon);
  if (hexAreaM2 > 0) {
    tractGeojson.features.forEach(tract => {
      const intersection = turf.intersect(turf.featureCollection([hexPolygon, tract]));
      if (intersection) {
        const intersectionArea = turf.area(intersection);
        const estimatedPopulation = (intersectionArea / 1e6) * tract.properties.DENSITY;
        totalPopulation += estimatedPopulation;
      }
    });
  }
  const density = totalPopulation / (hexAreaM2 / 1e6);
  return { hex, density };
}

function processChunk() {
  // Read the hexagon chunk from the environment.
  let hexChunk;
  try {
    hexChunk = JSON.parse(process.env.HEX_CHUNK);
  } catch (err) {
    process.send({ error: `Failed to parse HEX_CHUNK: ${err}` });
    process.exit(1);
  }

  // Load the tractGeojson from the temporary file.
  const tractFile = process.env.TRACT_FILE;
  let tractGeojson;
  try {
    const fileContent = fs.readFileSync(tractFile, 'utf8');
    tractGeojson = JSON.parse(fileContent);
  } catch (err) {
    process.send({ error: `Failed to load tractGeojson from ${tractFile}: ${err}` });
    process.exit(1);
  }

  const results = [];
  hexChunk.forEach(hex => {
    const result = computeHexDensity(hex, tractGeojson);
    results.push(result);
  });
  return results;
}

try {
  const results = processChunk();
  process.send(results);
} catch (err) {
  process.send({ error: err.toString() });
  process.exit(1);
}
