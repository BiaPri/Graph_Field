//                                                              *** DATABASE INPUT ***

// Constraints
CREATE CONSTRAINT ON (s:Station) ASSERT s.code IS UNIQUE;
CREATE CONSTRAINT ON (t:Train) ASSERT t.number IS UNIQUE;
CREATE CONSTRAINT ON (s:Stop) ASSERT s.id IS UNIQUE; 


// Upload Station
CALL apoc.periodic.iterate(
"CALL apoc.load.json('https://raw.githubusercontent.com/BiaPri/Graph_Field/master/data/Indian_Railway/stations.json')
YIELD value
UNWIND value.features as station
RETURN station",
"CREATE (s:Station {name: station.properties.name, state: station.properties.state,
                   code: station.properties.code, address: station.properties.address,
                   zone: station.properties.zone})",
{batchSize: 100});


// Upload Trains (For the moment: name, from_station_code, to_station_code)
CALL apoc.periodic.iterate(
"CALL apoc.load.json('https://raw.githubusercontent.com/BiaPri/Graph_Field/master/data/Indian_Railway/trains.json')
YIELD value
UNWIND value.features as train
RETURN train",
"CREATE (t:Train {name: train.properties.name, from_station_code: train.properties.from_station_code,
                   to_station_code: train.properties.to_station_code, number: train.properties.number})",
{batchSize: 100});


// Connections FROM & TO
MATCH (t:Train)
MATCH (s1:Station {code: t.from_station_code}), (s2:Station {code: t.to_station_code})
MERGE (s1)<-[:FROM]-(t)-[:TO]->(s2)



// Upload STOPS (Find another way) Too many nodes for Aura DB
CALL apoc.periodic.iterate(
"CALL apoc.load.json('https://raw.githubusercontent.com/BiaPri/Graph_Field/master/data/Indian_Railway/schedules.json')
YIELD value
UNWIND value as stop
RETURN stop",
"CREATE (s:Stop {id: stop.id, train_number: stop.train_number, station_code: stop.station_code, 
                 departure: stop.departure})",
{batchSize: 600});

MATCH (st1: Stops), (st2: Stops)
WHERE st1.train_number = st2.train_number AND time(st1.departure) > time(st2.departure)
MERGE (st1)-[:NEXT]->(st2)



// Upload STOPS (For the moment: nothing) Takes a long time
CALL apoc.periodic.iterate(
"CALL apoc.load.json('https://raw.githubusercontent.com/BiaPri/Graph_Field/master/data/Indian_Railway/schedules.json')
YIELD value
UNWIND value as stop
RETURN stop",
"MATCH (t:Train {name: stop.train_name})
MATCH (s:Station {code: stop.station_code})
MERGE (t)-[:STOPS]->(s)",
{batchSize: 300});



// Delete All
MATCH (n)
DETACH DELETE n;


//FOR TRIP DURATION duration: time(apoc.text.join([train.properties.duration_h, train.properties.duration_m],':'))


