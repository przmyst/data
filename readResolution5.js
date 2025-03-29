// readResolution5.js
const fs = require('fs').promises;
const path = require('path');

const admin = require('firebase-admin');
const serviceAccount = require('./startrail-6bb13-firebase-adminsdk-6qq9g-aacf8e052f.json');
admin.initializeApp({
    credential: admin.credential.cert(serviceAccount)
});
const db = admin.firestore();
// Initialize Firebase Admin SDK

async function readResolution5Files() {
    const densityDir = path.join(__dirname, 'density');

    try {
        // Read the list of entries in the density directory
        const fipsFolders = await fs.readdir(densityDir, { withFileTypes: true });

        for (const entry of fipsFolders) {
            if (entry.isDirectory()) {
                const fipsDir = path.join(densityDir, entry.name);
                const resolutionDir = path.join(fipsDir, '7'); // Look for resolution 5

                try {
                    // Check if the resolution folder exists
                    const stats = await fs.stat(resolutionDir);
                    if (stats.isDirectory()) {
                        // Read the JSON files in the resolution folder
                        const files = await fs.readdir(resolutionDir);
                        for (const file of files) {
                            if (path.extname(file) === '.json') {
                                const filePath = path.join(resolutionDir, file);
                                try {
                                    const fileContent = await fs.readFile(filePath, 'utf8');
                                    const jsonData = JSON.parse(fileContent);
                                    const batch = db.batch();
                                    const hexesCollection = db.collection('density');

                                    for (const hexId in jsonData) {
                                        const docRef = hexesCollection.doc(hexId);
                                        if(jsonData[hexId].density) {
                                            batch.set(docRef, {
                                                density: jsonData[hexId].density
                                            });
                                            console.log(jsonData[hexId])
                                        }else{
                                            console.log(file)
                                        }

                                    }

                                    await batch.commit();
                                } catch (fileErr) {
                                    console.error(`Error reading/parsing ${filePath}:`, fileErr);
                                }
                            }
                        }
                    }
                } catch (err) {
                    // If the resolution directory doesn't exist, we simply continue
                    console.warn(`No resolution 5 folder found in ${fipsDir}`);
                }
            }
        }
    } catch (err) {
        console.error('Error reading density directory:', err);
    }
}

readResolution5Files();
