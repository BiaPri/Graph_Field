//                                                              *** DATABASE INPUT ***

// Constraints
CREATE CONSTRAINT ON (s:Station) ASSERT s.code IS UNIQUE;
CREATE CONSTRAINT ON (t:Train) ASSERT t.name IS UNIQUE;


// Upload Station
CALL apoc.periodic.iterate(
"CALL apoc.load.json('https://raw.githubusercontent.com/BiaPri/Graph_Field/master/data/Indian_Railway/stations.json')
YIELD value
UNWIND value.features.properties as station
RETURN station",
"CREATE (s:Station {name: station.name, state: station.state,
                   code: station.code, address: station.address,
                   zone: station.zone})",
{batchSize: 600});


// Upload Trains (For the moment: name, from_station_code, to_station_code, departure, arrival, duration)
CALL apoc.periodic.iterate(
"CALL apoc.load.json('https://raw.githubusercontent.com/BiaPri/Graph_Field/master/data/Indian_Railway/trains.json')
YIELD value
UNWIND value.features.properties as train
RETURN train",
"CREATE (s:Station {name: station.name, state: station.state,
                   code: station.code, address: station.address,
                   zone: station.zone})",
{batchSize: 600});



// Delete All
MATCH (n)
DETACH DELETE n;



