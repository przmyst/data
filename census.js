const fs = require('fs');
const path = require('path');
const csv = require('csv-parser');

// Mapping of state abbreviations to FIPS codes
const stateFipsMapping = {
    AL: "01",
    AK: "02",
    AZ: "04",
    AR: "05",
    CA: "06",
    CO: "08",
    CT: "09",
    DE: "10",
    DC: "11",
    FL: "12",
    GA: "13",
    HI: "15",
    ID: "16",
    IL: "17",
    IN: "18",
    IA: "19",
    KS: "20",
    KY: "21",
    LA: "22",
    ME: "23",
    MD: "24",
    MA: "25",
    MI: "26",
    MN: "27",
    MS: "28",
    MO: "29",
    MT: "30",
    NE: "31",
    NV: "32",
    NH: "33",
    NJ: "34",
    NM: "35",
    NY: "36",
    NC: "37",
    ND: "38",
    OH: "39",
    OK: "40",
    OR: "41",
    PA: "42",
    RI: "44",
    SC: "45",
    SD: "46",
    TN: "47",
    TX: "48",
    UT: "49",
    VT: "50",
    VA: "51",
    WA: "53",
    WV: "54",
    WI: "55",
    WY: "56",
    PR: "72"
};

// Function to process a single CSV file
function processFile(filePath) {
    const selectedRows = [];
    const uniqueTracts = new Set();

    fs.createReadStream(filePath)
        .pipe(csv())
        .on('data', (row) => {
            // Process rows that have a TRACT value and ensure uniqueness
            if (row.TRACT && !uniqueTracts.has(row.TRACT)) {
                uniqueTracts.add(row.TRACT);
                selectedRows.push({
                    TRACT: parseInt(row.TRACT),
                    POP100: parseInt(row.POP100),
                    AREALAND: row.AREALAND
                });
            }
        })
        .on('end', () => {
            // Sort rows in descending order by POP100
            selectedRows.sort((a, b) => b.POP100 - a.POP100);

            // Build CSV content with a header
            const header = 'TRACT,POP100,AREALAND\n';
            const csvData = selectedRows
                .map(row => `${row.TRACT},${row.POP100},${row.AREALAND}`)
                .join('\n');
            const outputCsv = header + csvData;

            // Extract state abbreviation from the file name
            const parsedPath = path.parse(filePath);
            const fileNameParts = parsedPath.name.split('_');
            const stateAbbr = fileNameParts[fileNameParts.length - 1];
            const fips = stateFipsMapping[stateAbbr];
            const outputFileName = path.join(`${parsedPath.dir}`,'density', fips + ".csv")

            // Write the processed CSV to file
            fs.writeFile(outputFileName, outputCsv, (err) => {
                if (err) {
                    console.error(`Error writing CSV file for ${filePath}:`, err);
                } else {
                    console.log(`CSV file for ${filePath} successfully exported as ${outputFileName}`);
                }
            });
        });
}

// Read all files in the current directory
fs.readdir('./census', (err, files) => {
    if (err) {
        console.error('Error reading directory:', err);
        return;
    }

    // Filter for state CSV files with names like "StateName_XX.csv"
    const stateFiles = files.filter(file => /^[A-Za-z_]+_[A-Za-z]{2}\.csv$/.test(file));

    if (stateFiles.length === 0) {
        console.log('No state CSV files found.');
        return;
    }

    // Process each state file individually
    stateFiles.forEach(file => {
        console.log(`Processing ${file}...`);
        processFile(`./census/${file}`);
    });
});
