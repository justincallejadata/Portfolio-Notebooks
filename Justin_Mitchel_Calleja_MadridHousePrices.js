db.madridhouseprices.find({})
   .projection({})
   .sort({})
   .limit(0)
   
db.madridhouseprices.deleteMany({ 
    $or: [ 
        { price: { $lt: 50000 } },
        { price: { $gt: 10000000 } },
        { m2: { $lt: 10 } },
        { m2: { $exists: false } }
    ]
});


db.madridhouseprices.updateMany({}, [
    { $set: { price_per_m2: { $divide: ["$price", "$m2"] } } }
]);


db.madridhouseprices.find({})
   .projection({ neighborhood: 1, price_per_m2: 1, _id: 0 })
   .sort({ price_per_m2: 1 })
   .limit(1);


db.madridhouseprices.find({})
   .projection({ neighborhood: 1, price_per_m2: 1, _id: 0 })
   .sort({ price_per_m2: -1 })
   .limit(1);


db.madridhouseprices.find({})
   .projection({ neighborhood: 1, price: 1, m2: 1, rooms: 1, _id: 0 })
   .sort({ price: 1 })
   .limit(1);


db.madridhouseprices.find({})
   .projection({ neighborhood: 1, price: 1, m2: 1, rooms: 1, _id: 0 })
   .sort({ price: -1 })
   .limit(1);
   

db.madridhouseprices.aggregate([
    { $group: {
        _id: { district: "$district", neighborhood: "$neighborhood", rooms: "$rooms" },
        avgPricePerFlat: { $avg: "$price" }
    }},
    { $sort: { "_id.district": 1, "_id.neighborhood": 1, "_id.rooms": 1 } }
]);

db.madridhouseprices.aggregate([
    { $group: {
        _id: "$district",
        avgPricePerM2: { $avg: "$price_per_m2" }
    }},
    { $sort: { avgPricePerM2: -1 } }
]);

db.madridhouseprices.aggregate([
    { $group: {
        _id: { district: "$district", neighborhood: "$neighborhood", rooms: "$rooms", houseType: "$house_type" },
        avgPrice: { $avg: "$price" }
    }},
    { $sort: { "_id.district": 1, "_id.neighborhood": 1, "_id.rooms": 1 } }
]);

const maxDistrict = db.madridhouseprices.aggregate([
    { $group: { _id: "$district", avgPrice: { $avg: "$price" } } },
    { $sort: { avgPrice: -1 } },
    { $limit: 1 }
]).toArray()[0];

const maxPrice = maxDistrict.avgPrice;

db.madridhouseprices.aggregate([
    { $group: { _id: "$district", avgPrice: { $avg: "$price" } } },
    { $project: { _id: 1, avgPrice: 1, priceRatio: { $divide: ["$avgPrice", maxPrice] } } },
    { $sort: { priceRatio: -1 } }
]);

db.madridhouseprices.aggregate([
    { $group: { _id: { district: "$district", houseType: "$house_type" }, avgPrice: { $avg: "$price" } } },
    { $sort: { "_id.district": 1, avgPrice: -1 } }
]);

db.madridhouseprices.aggregate([
    { $group: { _id: { district: "$district", rooms: "$rooms" }, avgPrice: { $avg: "$price" } } },
    { $sort: { "_id.district": 1, avgPrice: -1 } }
]);

db.madridhouseprices.aggregate([
    { $group: { _id: "$district", avgPrice: { $avg: "$price" }, maxM2: { $max: "$m2" } } },
    { $sort: { avgPrice: 1 } }
]);

db.madridhouseprices.aggregate([
    { $group: { _id: { district: "$district", garage: "$garage" }, avgPrice: { $avg: "$price" } } },
    { $sort: { "_id.district": 1, "_id.garage": 1 } }
]);

db.madridhouseprices.aggregate([
    { $group: { _id: { district: "$district", neighborhood: "$neighborhood" }, avgPricePerM2: { $avg: "$price_per_m2" } } },
    { $sort: { "_id.district": 1, avgPricePerM2: 1 } }
]);

db.madridhouseprices.aggregate([
    { $group: { _id: { neighborhood: "$neighborhood", rooms: "$rooms" }, avgPricePerM2: { $avg: "$price_per_m2" } } },
    { $sort: { avgPricePerM2: -1 } }
]);

