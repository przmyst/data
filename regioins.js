const { initializeApp, cert } = require('firebase-admin/app');
const { getFirestore } = require('firebase-admin/firestore');

initializeApp({
    credential: cert(require('./startrail-6bb13-firebase-adminsdk-6qq9g-2ebaf0d8e0.json'))
});

const db = getFirestore();
const regions = require('./regions.json');

async function storeRegions() {
    // Use a batch to commit writes efficiently (max 500 operations per batch)
    let batch = db.batch();
    let batchCounter = 0;
    const BATCH_LIMIT = 500;

    // Helper to commit batch if near the limit and reset the counter.
    async function commitBatch() {
        await batch.commit();
        console.log(`Committed batch with ${batchCounter} operations.`);
        batch = db.batch();
        batchCounter = 0;
    }

    for (const country of regions) {
        // Use the iso2 code as document id if available; fallback to country.id.
        const countryId = country.iso2 || country.id.toString();
        const countryRef = db.collection('countries').doc(countryId);

        // Remove states from the country data; we'll store them separately.
        const { states, ...countryData } = country;

        batch.set(countryRef, countryData);
        batchCounter++;

        // If the country has states, store each state in a "states" subcollection.
        if (states && states.length > 0) {
            for (const state of states) {
                // Remove cities from the state data; these will be stored in their own subcollection.
                const { cities, ...stateData } = state;
                const stateId = state.state_code || state.id.toString();
                const stateRef = countryRef.collection('states').doc(stateId);
                batch.set(stateRef, stateData);
                batchCounter++;

                // If the state has cities, store each in a "cities" subcollection under the state.
                if (cities && cities.length > 0) {
                    for (const city of cities) {
                        const cityId = city.id.toString();
                        const cityRef = stateRef.collection('cities').doc(cityId);
                        batch.set(cityRef, city);
                        batchCounter++;

                        // Commit the batch if nearing the limit.
                        if (batchCounter >= BATCH_LIMIT - 1) {
                            await commitBatch();
                        }
                    }
                }

                // Check batch limit after each state.
                if (batchCounter >= BATCH_LIMIT - 1) {
                    await commitBatch();
                }
            }
        }

        // Check batch limit after each country.
        if (batchCounter >= BATCH_LIMIT - 1) {
            await commitBatch();
        }
    }

    // Commit any remaining operations.
    if (batchCounter > 0) {
        await commitBatch();
    }

    console.log('All regions, states, and cities have been stored in Firestore.');
}

storeRegions().catch(console.error);
