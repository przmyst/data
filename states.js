const fs = require('fs');
const path = require('path');

// Set the input file to "us-state-boundaries.geojson" (ensure it is in the same directory as this script)
const inputFile = path.join(__dirname, 'us-state-boundaries.geojson');

fs.readFile(inputFile, 'utf8', (err, data) => {
    if (err) {
        console.error('Error reading file:', err);
        return;
    }

    let geojson;
    try {
        geojson = JSON.parse(data);
    } catch (parseErr) {
        console.error('Error parsing JSON:', parseErr);
        return;
    }

    // Verify that the GeoJSON contains a FeatureCollection with an array of features.
    if (!geojson.features || !Array.isArray(geojson.features)) {
        console.error('Invalid GeoJSON: No features array found.');
        return;
    }

    // Iterate over each feature and write a separate GeoJSON file for each state.
    geojson.features.forEach(feature => {
        console.log(feature)
        const gid = feature.properties && feature.properties.state;
        if (!gid) {
            console.warn('Skipping feature without a gid:', feature);
            return;
        }

        // Wrap the feature in a FeatureCollection.
        const outGeojson = {
            type: "FeatureCollection",
            features: [feature]
        };

        // Create output filename based on the gid.
        const outputFilename = path.join(`${__dirname}`, 'states', `${gid}.geojson`);

        fs.writeFile(outputFilename, JSON.stringify(outGeojson, null, 2), 'utf8', err => {
            if (err) {
                console.error(`Error writing file ${outputFilename}:`, err);
            } else {
                console.log(`Successfully wrote ${outputFilename}`);
            }
        });
    });
});
