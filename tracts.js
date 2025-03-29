const axios = require('axios');
const fs = require('fs');
const path = require('path');

async function downloadFile(url, outputPath) {
    const writer = fs.createWriteStream(outputPath);
    const response = await axios({
        url,
        method: 'GET',
        responseType: 'stream'
    });
    response.data.pipe(writer);
    return new Promise((resolve, reject) => {
        writer.on('finish', resolve);
        writer.on('error', reject);
    });
}

async function main() {
    const baseUrl = 'https://www2.census.gov/geo/tiger/TIGER2020/TRACT/';

    // Loop through state codes 01 to 51.
    for (let i = 1; i <= 57; i++) {
        const stateCode = i.toString().padStart(2, '0'); // e.g., "01", "02", ..., "51"
        const fileName = `tl_2020_${stateCode}_tract.zip`;
        const fileUrl = `${baseUrl}${fileName}`;

        console.log(`Downloading ${fileUrl} ...`);
        try {
            await downloadFile(fileUrl, path.join(`${__dirname}/tracts/2020`, fileName));
            console.log(`Downloaded ${fileName}`);
        } catch (error) {
            console.error(`Error downloading ${fileName}:`, error.message);
        }
    }
}

main();
