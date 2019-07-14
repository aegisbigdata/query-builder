const fs = require("fs");
const debug = false;
const template = JSON.parse(fs.readFileSync('tools/template.json'));
let ipynb = Object.assign({}, template.ipynb);

const cellFilenames = fs.readdirSync("cells/");

cellFilenames.forEach(function (filename) {
    if (filename.substr(-5) === ".json")
        return;
    if (filename.indexOf(".debug") > 0 && !debug)
        return;
    let cell = processCell(filename);
    let cellOptionsFile = filename.substr(0, filename.lastIndexOf("."));
    cellOptionsFile = `cells/${cellOptionsFile}.json`;
    if (fs.existsSync(cellOptionsFile)) {
        cell = Object.assign(cell, JSON.parse(fs.readFileSync(cellOptionsFile)))
    }
    ipynb.cells.push(cell);
});

const ipynbFormatted = JSON.stringify(ipynb, null, 1) + "\n";

fs.writeFile('QueryBuilder_v1.ipynb', ipynbFormatted, (err) => {
    if (err) throw err;
    console.log('Data written to file');
});


function processCell(filename) {
    let source = fs.readFileSync(`cells/${filename}`).toString()
        .replace(/\r/g, "")
        .replace(/"/g, "\"")
        .split("\n")
        .map(l => l + "\n");
    let cell = Object.assign({}, template.code);
    if (filename.substr(-3) === ".md") {
        cell = Object.assign({}, template.md);
    }
    cell.source = source;
    return cell;
}