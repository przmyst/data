//storeHexDensity.js
const cluster = require('cluster');
const numFips = 57;
const resolutions = [5,6,7];
const totalCpus = 180;
let cpuIndex = 0;

if (cluster.isMaster) {
    console.log(`Master process ${process.pid} is running`);

    // For each FIPS code and each resolution, fork a worker and assign a CPU index.
    for (let i = 1; i <= numFips; i++) {
        const fips = i.toString().padStart(2, '0');
        for (let resolution of resolutions) {
            cluster.fork({ FIPS: fips, RESOLUTION: resolution, CPU: cpuIndex });
            cpuIndex = (cpuIndex + 1) % totalCpus;
        }
    }

    cluster.on('exit', (worker, code, signal) => {
        console.log(`Worker ${worker.process.pid} finished with code ${code}`);
    });
} else {
    // Each worker process handles one FIPS file at one resolution.
    const processFips = require('./densityWorker.js');
    const fips = process.env.FIPS;
    console.log(`Worker ${process.pid} processing FIPS: ${fips} at resolution ${process.env.RESOLUTION}`);
    processFips(fips)
        .then(() => {
            console.log(`Worker ${process.pid} completed processing FIPS: ${fips} at resolution ${process.env.RESOLUTION}`);
            process.exit(0);
        })
        .catch(err => {
            console.error(`Error processing FIPS ${fips} in worker ${process.pid}:`, err);
            process.exit(1);
        });
}
