//                                                              *** DATABASE INPUT ***

// Constraints
CREATE CONSTRAINT ON (s:Station) ASSERT s.code IS UNIQUE;
CREATE CONSTRAINT ON (t:Train) ASSERT t.name IS UNIQUE; // Strange not all name are unique => Inforce with MERGE


// Upload Station
CALL apoc.periodic.iterate(
"CALL apoc.load.json('https://raw.githubusercontent.com/BiaPri/Graph_Field/master/data/Indian_Railway/stations.json')
YIELD value
UNWIND value.features as station
RETURN station",
"CREATE (s:Station {name: station.properties.name, state: station.properties.state,
                   code: station.properties.code, address: station.properties.address,
                   zone: station.properties.zone})",
{batchSize: 600});


// Upload Trains (For the moment: name, from_station_code, to_station_code)
CALL apoc.periodic.iterate(
"CALL apoc.load.json('https://raw.githubusercontent.com/BiaPri/Graph_Field/master/data/Indian_Railway/trains.json')
YIELD value
UNWIND value.features as train
RETURN train",
"MERGE (t:Train {name: train.properties.name, from_station_code: train.properties.from_station_code,
                   to_station_code: train.properties.to_station_code})",
{batchSize: 500});


// Upload STOPS (For the moment: nothing)
CALL apoc.periodic.iterate(
"CALL apoc.load.json('https://raw.githubusercontent.com/BiaPri/Graph_Field/master/data/Indian_Railway/schedules.json')
YIELD value
UNWIND value as stop
RETURN stop",
"MATCH (t:Train {name: stop.train_name})
MATCH (s:Station {code: stop.station_code})
CREATE (t)-[:STOPS]->(s)",
{batchSize: 2000});


// Delete All
MATCH (n)
DETACH DELETE n;


//FOR TRIP DURATION duration: time(apoc.text.join([train.properties.duration_h, train.properties.duration_m],':'))


