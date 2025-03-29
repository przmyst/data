// storeHexDensity.js

const fs = require('fs');
const fsPromises = require('fs').promises;
const path = require('path');
const os = require('os');
const Papa = require('papaparse');
const shapefile = require('shapefile');
const unzipper = require('unzipper');
const h3 = require('h3-js');
const turf = require('@turf/turf');
const admin = require('firebase-admin');

// Initialize Firebase Admin SDK
const serviceAccount = require('./startrail-6bb13-firebase-adminsdk-6qq9g-2ebaf0d8e0.json');
const {featureCollection} = require("@turf/turf");
admin.initializeApp({
    credential: admin.credential.cert(serviceAccount)
});
const db = admin.firestore();

// Helper: Extract a zipped shapefile to a temporary directory.
async function extractZip(zipFilePath) {
    const tempDir = await fsPromises.mkdtemp(path.join(os.tmpdir(), 'shapefile-'));
    await fs.createReadStream(zipFilePath)
        .pipe(unzipper.Extract({ path: tempDir }))
        .promise();
    return tempDir;
}

// Helper: Compute the density for a given hex cell by intersecting its polygon
// with all census tracts. The population estimate for each intersection is based on the tract's
// density (pop per km²) and the intersection area (converted from m² to km²).
function computeHexDensity(hexPolygon, censusTracts) {
    let totalPopulation = 0;
    const hexAreaM2 = turf.area(hexPolygon);
    if (hexAreaM2 === 0) return null;

    censusTracts.forEach(tract => {
        const intersection = turf.intersect(turf.featureCollection([hexPolygon, tract]));
        if (intersection) {
            const intersectionArea = turf.area(intersection); // in m²
            // Estimated population from this intersection:
            // (intersection area in km²) * tract density (pop per km²)
            const estimatedPopulation = (intersectionArea / 1e6) * tract.properties.DENSITY;
            totalPopulation += estimatedPopulation;
        }
    });

    // Compute density as population per km².
    const hexDensity = totalPopulation / (hexAreaM2 / 1e6);
    return hexDensity;
}

async function main() {
    // === 1. Load Census Tract Population Data ===
    console.log('Loading census CSV data...');
    const csvFilePath = './nv_tract_pop.csv';
    const csvContent = fs.readFileSync(csvFilePath, 'utf8');
    const parsedCSV = Papa.parse(csvContent, { header: true });

    // Build a mapping of padded tract IDs to population and area information.
    // Density is computed as (population / areaLand) * 1e6 to get pop per km².
    const tractData = {};
    parsedCSV.data.forEach(row => {
        // Pad the TRACT value to 6 characters (e.g., "5858" becomes "005858")
        const paddedTract = row.TRACT.toString().padStart(6, '0');
        const population = parseFloat(row.POP100);
        const areaLand = parseFloat(row.AREALAND); // assuming area in m²
        if (areaLand > 0) { // Avoid division by zero
            const density = (population / areaLand) * 1e6;
            tractData[paddedTract] = { density, population, areaLand };
        }
    });

    // === 2. Load Census Tract Shapefile ===
    console.log('Extracting and loading census shapefile...');
    const zipFilePath = './tl_2020_32_tract.zip';
    const tempDir = await extractZip(zipFilePath);
    // Assuming the extracted files are named 'tl_2020_32_tract.shp' and 'tl_2020_32_tract.dbf'
    const shpFilePath = path.join(tempDir, 'tl_2020_32_tract.shp');
    const dbfFilePath = path.join(tempDir, 'tl_2020_32_tract.dbf');

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

    // === 3. Define Reno Boundary and Compute Hexagons ===
    // Reno boundary coordinates in [lng, lat] order.
    const renoBoundary = [
        [-120.00011679161832, 39.73803782354049], // Top-left
        [-119.61696859825895, 39.74648552089187], // Top-right
        [-119.57302328575895, 39.40777044186991], // Bottom-right
        [-119.99599691857145, 39.40140373216833], // Bottom-left
        [-120.00011679161832, 39.73803782354049]  // Close polygon
    ];
    // h3-js expects the polygon in [lat, lng] order.
    const renoBoundaryLatLng = renoBoundary.map(coord => [coord[1], coord[0]]);
    const polygon = [renoBoundaryLatLng];

    // Set the desired resolution (for example, 5).
    const resolution = 8;
    console.log('Computing hexagons for Reno...');
    const hexagons = h3.polygonToCells(polygon, resolution);
    console.log(`Computed ${hexagons.length} hexagons.`);

    // === 4. Compute Density for Each Hexagon Directly ===
    const hexData = {};
    for (const hex of hexagons) {
        // Get the boundary of the hexagon in GeoJSON order ([lng, lat]).
        let boundary = h3.cellToBoundary(hex, true);
        // Close the polygon by appending the first coordinate.
        if (boundary.length > 0) {
            boundary.push(boundary[0]);
        }
        const hexPolygon = turf.polygon([boundary]);

        // Compute the hex density by intersecting it with all census tracts.
        const hexDensity = computeHexDensity(hexPolygon, tractGeojson.features);

        // Store the hex id and its computed density.
        hexData[hex] = {
            hex,
            density: hexDensity
        };
    }

    // === 5. Store Hexagon Data in Firebase Firestore ===
    console.log('Storing hexagon data to Firebase Firestore...');
    const batch = db.batch();
    const hexesCollection = db.collection('density');

    for (const hexId in hexData) {
        const docRef = hexesCollection.doc(hexId);
        batch.set(docRef, hexData[hexId]);
    }

    await batch.commit();
    console.log('Successfully stored hexagon density data!');
}

main().catch(error => {
    console.error('Error during processing:', error);
});
