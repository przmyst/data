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

const DEBUG = process.env.DEBUG === 'true';

// --- Optionally pin this worker to a single CPU ---
const cpuIndex = process.env.CPU ? parseInt(process.env.CPU) : 0;
// try {
//     execSync(`taskset -p -c ${cpuIndex} ${process.pid}`);
//     console.log(`Worker ${process.pid} pinned to CPU ${cpuIndex}`);
// } catch (err) {
//     console.error("Error pinning process to CPU", err);
// }

// Use the RESOLUTION from environment (default to 7 if not provided)
const resolution = process.env.RESOLUTION ? parseInt(process.env.RESOLUTION) : 7;

async function extractZip(zipFilePath) {
    console.log(`Extracting zip file from: ${zipFilePath}`);
    const tempDir = await fsPromises.mkdtemp(path.join(os.tmpdir(), 'shapefile-'));
    await fs.createReadStream(zipFilePath)
        .pipe(unzipper.Extract({ path: tempDir }))
        .promise();
    console.log(`Extracted files to temporary directory: ${tempDir}`);
    return tempDir;
}

async function processFips(fips) {
    // Set the output directory and file paths for our two GeoJSON outputs.
    const outputDir = path.join(__dirname, 'density', `${fips}/${resolution}`);
    const hexOutputPath = path.join(outputDir, `${fips}_hexagons.geojson`);
    const tractsOutputPath = path.join(outputDir, `${fips}_tracts.geojson`);

    // If both outputs already exist, we can skip processing.
    if (fs.existsSync(hexOutputPath) && fs.existsSync(tractsOutputPath)) {
        console.log(`GeoJSON files for FIPS ${fips} at resolution ${resolution} already exist. Skipping processing.`);
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
        if (DEBUG) {
            console.log(`Row for tract ${paddedTract}: Population=${population}, Area=${areaLand}`);
        }
        if (areaLand > 0) {
            const density = (population / areaLand) * 1e6;
            tractData[paddedTract] = { density, population, areaLand };
            if (DEBUG) {
                console.log(`Computed density for tract ${paddedTract}: ${density.toFixed(2)} per km²`);
            }
        } else {
            console.warn(`Skipping tract ${paddedTract} due to zero or invalid land area.`);
        }
    });
    console.log(`Loaded density data for ${Object.keys(tractData).length} tracts.`);

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
            if (DEBUG) {
                const geoid = result.value.properties.GEOID;
                console.log(`Loaded tract feature with GEOID: ${geoid}`);
            }
        }
        console.log(`Loaded ${tractGeojson.features.length} tract features from shapefile.`);
    } catch (error) {
        console.error('Error reading shapefile:', error);
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
            if (DEBUG) {
                console.log(`Assigned density ${feature.properties.DENSITY.toFixed(2)} to tract ${geoid}`);
            }
        } else if (DEBUG) {
            console.warn(`No density data found for tract ${geoid} (key: ${tractKey})`);
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
    if (DEBUG) {
        console.log(`State boundary loaded with ${stateBoundary.length} coordinates. First coordinate: ${JSON.stringify(stateBoundary[0])}`);
    }

    // h3-js expects polygon coordinates in [lat, lng] order.
    const stateBoundaryLatLng = stateBoundary.map(coord => [coord[1], coord[0]]);
    const polygon = [stateBoundaryLatLng];

    console.log(`Computing hexagons for state ${fips} at resolution ${resolution}...`);
    const hexagons = h3.polygonToCells(polygon, resolution);
    console.log(`Computed ${hexagons.length} hexagons.`);

    // === 4. Compute Hexagon Features and Build GeoJSON ===
    const hexagonGeojson = { type: "FeatureCollection", features: [] };
    let zeroDensityCount = 0;
    hexagons.forEach(hex => {
        if (DEBUG) {
            console.log(`\nProcessing hexagon: ${hex}`);
        }
        // Get the boundary in [lat, lng] order and close the polygon.
        let boundary = h3.cellToBoundary(hex, true).map(coord => [coord[1], coord[0]]);
        if (boundary.length > 0) boundary.push(boundary[0]);
        const hexPolygon = turf.polygon([boundary]);
        const hexAreaM2 = turf.area(hexPolygon);
        if (DEBUG) {
            console.log(`Hexagon ${hex} area: ${hexAreaM2.toFixed(2)} m²`);
        }
        let totalPopulation = 0;
        if (hexAreaM2 > 0) {
            tractGeojson.features.forEach(tract => {
                // Compute the intersection between the hexagon and the tract.
                const intersection = turf.intersect(hexPolygon, tract);
                if (intersection) {
                    const intersectionArea = turf.area(intersection);
                    // Estimate the population for the intersected area using tract density.
                    const estimatedPopulation = (intersectionArea / 1e6) * tract.properties.DENSITY;
                    totalPopulation += estimatedPopulation;
                    if (DEBUG) {
                        console.log(`Hex ${hex} & tract ${tract.properties.GEOID}: intersection area=${intersectionArea.toFixed(2)} m², tract density=${tract.properties.DENSITY.toFixed(2)}, estimated pop=${estimatedPopulation.toFixed(2)}`);
                    }
                } else if (DEBUG) {
                    console.log(`No intersection between hex ${hex} and tract ${tract.properties.GEOID}`);
                }
            });
        } else {
            console.warn(`Hexagon ${hex} has zero or invalid area.`);
        }
        const density = totalPopulation / (hexAreaM2 / 1e6);
        if (density === 0) {
            zeroDensityCount++;
        }
        const hexFeature = {
            type: "Feature",
            properties: {
                hex_id: hex,
                density: density,
                estimated_population: totalPopulation
            },
            geometry: hexPolygon.geometry
        };
        hexagonGeojson.features.push(hexFeature);
        if (DEBUG) {
            console.log(`Hex ${hex}: total estimated population=${totalPopulation.toFixed(2)}, computed density=${density.toFixed(2)} per km²`);
        }
    });
    console.log(`Finished processing hexagons. ${zeroDensityCount} hexagons computed with zero density.`);

    // === 5. Save the Hexagon and Tract GeoJSON Files ===
    if (!fs.existsSync(outputDir)) {
        fs.mkdirSync(outputDir, { recursive: true });
        console.log(`Created output directory: ${outputDir}`);
    }
    fs.writeFileSync(hexOutputPath, JSON.stringify(hexagonGeojson, null, 2));
    console.log(`Successfully stored hexagon GeoJSON data for ${fips} at resolution ${resolution} to ${hexOutputPath}`);
    fs.writeFileSync(tractsOutputPath, JSON.stringify(tractGeojson, null, 2));
    console.log(`Successfully stored tract GeoJSON data for ${fips} to ${tractsOutputPath}`);
}

module.exports = processFips;
