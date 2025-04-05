// hexWorker.js
const { parentPort, workerData } = require('worker_threads');
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

async function processChunk() {
    const { hexagons, tractGeojson } = workerData;
    const results = [];
    for (const hex of hexagons) {
        const result = computeHexDensity(hex, tractGeojson);
        results.push(result);
    }
    return results;
}

processChunk()
    .then(results => {
        parentPort.postMessage(results);
    })
    .catch(err => {
        parentPort.postMessage({ error: err.toString() });
    });
