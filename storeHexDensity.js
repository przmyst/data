// storeHexDensity.js
const cluster = require('cluster');
const os = require('os');

// List the FIPS codes for states that still need processing.
const missingStates = ['02', '06', '30', '41', '48'];
const resolutions = [9];

// Use all available CPU cores.
const totalCpus = os.cpus().length;
let cpuIndex = 0;

if (cluster.isMaster) {
    console.log(`Master process ${process.pid} is running on ${totalCpus} cores`);

    // Fork a worker for each missing state/resolution combination.
    missingStates.forEach(fips => {
        resolutions.forEach(resolution => {
            cluster.fork({ FIPS: fips, RESOLUTION: resolution, CPU: cpuIndex });
            cpuIndex = (cpuIndex + 1) % totalCpus;
        });
    });

    cluster.on('exit', (worker, code, signal) => {
        console.log(`Worker ${worker.process.pid} finished with code ${code}`);
    });
} else {
    // Each worker handles one state (FIPS) at one resolution.
    const processFips = require('./densityWorker.js');
    const fips = process.env.FIPS;
    console.log(
        `Worker ${process.pid} processing FIPS: ${fips} at resolution ${process.env.RESOLUTION}`
    );
    processFips(fips)
        .then(() => {
            console.log(
                `Worker ${process.pid} completed processing FIPS: ${fips} at resolution ${process.env.RESOLUTION}`
            );
            process.exit(0);
        })
        .catch(err => {
            console.error(
                `Error processing FIPS ${fips} in worker ${process.pid}:`,
                err
            );
            process.exit(1);
        });
}
