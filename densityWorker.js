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
const { execSync } = require('child_process');

// --- Pin this worker to a single CPU ---
const cpuIndex = process.env.CPU ? parseInt(process.env.CPU) : 0;
try {
    // Bind the current process to the specified CPU.
    execSync(`taskset -p -c ${cpuIndex} ${process.pid}`);
    console.log(`Worker ${process.pid} pinned to CPU ${cpuIndex}`);
} catch (err) {
    console.error("Error pinning process to CPU", err);
}

// Use the RESOLUTION from environment (default to 7 if not provided)
const resolution = process.env.RESOLUTION ? parseInt(process.env.RESOLUTION) : 7;

async function extractZip(zipFilePath) {
    const tempDir = await fsPromises.mkdtemp(path.join(os.tmpdir(), 'shapefile-'));
    await fs.createReadStream(zipFilePath)
        .pipe(unzipper.Extract({ path: tempDir }))
        .promise();
    return tempDir;
}

async function processFips(fips) {
    // === Check if file has already been processed ===
    const outputDir = path.join(__dirname, 'density', `${fips}/${resolution}`);
    const outputFilePath = path.join(outputDir, `${fips}.json`);
    if (fs.existsSync(outputFilePath)) {
        console.log(`Hexagon density data for FIPS ${fips} at resolution ${resolution} already exists at ${outputFilePath}. Skipping processing.`);
        return;
    }

    console.log(`\n=== Processing FIPS: ${fips} at resolution ${resolution} ===`);

    // === 1. Load Census Tract Population Data ===
    console.log('Loading census CSV data...');
    const csvFilePath = `./census/density/${fips}.csv`;
    if (!fs.existsSync(csvFilePath)) {
        console.error(`CSV file for FIPS ${fips} not found at ${csvFilePath}`);
        return;
    }
    const csvContent = fs.readFileSync(csvFilePath, 'utf8');
    const parsedCSV = Papa.parse(csvContent, { header: true });
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

    // === 2. Load Census Tract Shapefile ===
    console.log('Extracting and loading census shapefile...');
    const zipFilePath = `./tracts/2020/tl_2020_${fips}_tract.zip`;
    if (!fs.existsSync(zipFilePath)) {
        console.error(`Shapefile for FIPS ${fips} not found at ${zipFilePath}`);
        return;
    }
    const tempDir = await extractZip(zipFilePath);
    const shpFilePath = path.join(tempDir, `tl_2020_${fips}_tract.shp`);
    const dbfFilePath = path.join(tempDir, `tl_2020_${fips}_tract.dbf`);

    const tractGeojson = { type: "FeatureCollection", features: [] };
    try {
        const source = await shapefile.open(shpFilePath, dbfFilePath);
        while (true) {
            const result = await source.read();
            if (result.done) break;
            tractGeojson.features.push(result.value);
        }
    } catch (error) {
        console.error('Error reading shapefile:', error);
        return;
    }

    // Attach density properties to each tract using the last 6 characters of GEOID.
    tractGeojson.features.forEach(feature => {
        const geoid = feature.properties.GEOID;
        const tractKey = geoid.slice(-6);
        if (tractData[tractKey]) {
            feature.properties.DENSITY = tractData[tractKey].density;
            feature.properties.POPULATION = tractData[tractKey].population;
            feature.properties.AREALAND = tractData[tractKey].areaLand;
        }
    });

    // === 3. Define State Boundary and Compute Hexagons ===
    console.log('Loading state boundary...');
    const stateGeoJSONPath = path.join(__dirname, 'states', `${fips}.geojson`);
    let stateGeoJSON;
    try {
        const stateContent = fs.readFileSync(stateGeoJSONPath, 'utf8');
        stateGeoJSON = JSON.parse(stateContent);
    } catch (error) {
        console.error('Error reading state boundary GeoJSON:', error);
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
            console.error('Unsupported geometry type:', feature.geometry.type);
            return;
        }
    } else if (stateGeoJSON.type === 'Feature') {
        if (stateGeoJSON.geometry.type === 'Polygon') {
            stateBoundary = stateGeoJSON.geometry.coordinates[0];
        } else if (stateGeoJSON.geometry.type === 'MultiPolygon') {
            stateBoundary = stateGeoJSON.geometry.coordinates[0][0];
        } else {
            console.error('Unsupported geometry type:', stateGeoJSON.geometry.type);
            return;
        }
    } else {
        console.error('Invalid GeoJSON format for state boundary.');
        return;
    }

    // h3-js expects polygon coordinates in [lat, lng] order.
    const stateBoundaryLatLng = stateBoundary.map(coord => [coord[1], coord[0]]);
    const polygon = [stateBoundaryLatLng];

    console.log(`Computing hexagons for state ${fips} at resolution ${resolution}...`);
    const hexagons = h3.polygonToCells(polygon, resolution);
    console.log(`Computed ${hexagons.length} hexagons.`);

    // === 4. Pre-filter Setup: Compute Hexagon Circumradius and Buffered Bounding Boxes for Tracts ===
    // Use the hexagon's edge length as the circumradius (distance from center to vertex)
    const hexEdgeLength = h3.getHexagonEdgeLengthAvg(resolution, 'm'); // in meters
    const hexCircumradius = hexEdgeLength; // for a regular hexagon, this is a safe expansion distance

    // For each tract, compute its bounding box and buffer it by the hexCircumradius.
    const tractBuffers = tractGeojson.features.map(tract => {
        const bbox = turf.bbox(tract); // [minX, minY, maxX, maxY]
        const bboxPolygon = turf.bboxPolygon(bbox);
        // Buffer the bounding box by the hexagon circumradius (in meters)
        const bufferedBbox = turf.buffer(bboxPolygon, hexCircumradius, { units: 'meters' });
        return { tract, bufferedBbox };
    });

    // === 5. Compute Density for Each Hexagon with Pre-filtering ===
    const hexData = {};
    hexagons.forEach(hex => {
        // Convert the hexagon to a polygon and compute its centroid.
        let boundary = h3.cellToBoundary(hex, true);
        if (boundary.length > 0) boundary.push(boundary[0]); // close the polygon
        const hexPolygon = turf.polygon([boundary]);
        const hexCentroid = turf.centroid(hexPolygon);

        let totalPopulation = 0;
        const hexAreaM2 = turf.area(hexPolygon);
        if (hexAreaM2 > 0) {
            // For each tract, first check if the hex centroid is in the tract’s buffered bounding box.
            tractBuffers.forEach(({ tract, bufferedBbox }) => {
                if (turf.booleanPointInPolygon(hexCentroid, bufferedBbox)) {
                    // Only perform the expensive intersection if the centroid is inside the buffered box.
                    const intersection = turf.intersect(turf.featureCollection([hexPolygon, tract]));
                    if (intersection) {
                        const intersectionArea = turf.area(intersection);
                        const estimatedPopulation = (intersectionArea / 1e6) * tract.properties.DENSITY;
                        totalPopulation += estimatedPopulation;
                    }
                }
            });
        }
        const density = totalPopulation / (hexAreaM2 / 1e6);
        hexData[hex] = { hex, density };
    });

    // === 6. Save Hexagon Density Data to a JSON File ===
    if (!fs.existsSync(outputDir)) {
        fs.mkdirSync(outputDir, { recursive: true });
    }
    fs.writeFileSync(outputFilePath, JSON.stringify(hexData, null, 2));
    console.log(`Successfully stored hexagon density data for ${fips} at resolution ${resolution} to ${outputFilePath}`);
}

module.exports = processFips;
