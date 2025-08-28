// Initialize inventory collection from sample JSON
db = db.getSiblingDB('inventorydb');
db.createCollection('products');
// Expecting a JSON array in the file
var data = cat('/docker-entrypoint-initdb.d/inventory_sample.json');
var docs = JSON.parse(data);
db.products.insertMany(docs);
