const fs = require('fs');
const csv = require('csv-parser');

const selectedRows = [];
const uniqueTracts = new Set();

fs.createReadStream('./Nevada_NV.csv')
    .pipe(csv())
    .on('data', (row) => {
        // Check if the row has a TRACT value and if it's unique
        if (row.TRACT && !uniqueTracts.has(row.TRACT)) {
            uniqueTracts.add(row.TRACT);
            // console.log(row);
            const selected = {
                TRACT: parseInt(row.TRACT),
                POP100: parseInt(row.POP100),
                AREALAND: row.AREALAND
            };
            selectedRows.push(selected);
        }
    })
    .on('end', () => {
        // Sort descending by POP100
        selectedRows.sort((a, b) => b.POP100 - a.POP100);

        // Build CSV content with a header
        const header = 'TRACT,POP100,AREALAND\n';
        const csvData = selectedRows
            .map(row => `${row.TRACT},${row.POP100},${row.AREALAND}`)
            .join('\n');
        const outputCsv = header + csvData;

        // Write CSV content to a file
        fs.writeFile('./output.csv', outputCsv, (err) => {
            if (err) {
                console.error('Error writing CSV file:', err);
            } else {
                console.log('CSV file successfully exported as output.csv');
            }
        });
    });
