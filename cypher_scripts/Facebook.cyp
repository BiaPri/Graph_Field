
CREATE CONSTRAINT ON (p:Page) ASSERT p.page_id IS UNIQUE

CALL apoc.periodic.iterate(
"CALL apoc.load.csv('https://raw.githubusercontent.com/BiaPri/Graph_Field/master/data/Facebook/musae_facebook_target.csv')
 YIELD map AS row RETURN row",
"CREATE (:Page {page_id: toInteger(row.id), facebook_id: toInteger(row.facebook_id), 
               page_name: row.page_name, page_type: row.page_type})",
 {batchSize: 150}
);

CALL apoc.periodic.iterate(
"CALL apoc.load.csv('https://raw.githubusercontent.com/BiaPri/Graph_Field/master/data/Facebook/musae_facebook_edges.csv')
 YIELD map AS row RETURN row",
"MATCH (p1:Page {page_id: toInteger(row.id_1)})
 MATCH (p2:Page {page_id: toInteger(row.id_2)})
 CREATE (p1)-[:CONNECTED]->(p2)",
 {batchSize: 400}
);

//Types = ["tvshow", "government", "company", "politician"]
MATCH (p:Page)
WHERE p.page_type = "tvshow"
SET p:Tvshow;
MATCH (p:Page)
WHERE p.page_type = "government"
SET p:Government;
MATCH (p:Page)
WHERE p.page_type = "company"
SET p:Company;
MATCH (p:Page)
WHERE p.page_type = "politician"
SET p:Politician;
MATCH (p:Page)
REMOVE p.page_type;

// EDA
MATCH (p:Company)-[:CONNECTED]->(:Company)
WITH COUNT(*) AS connections, p
RETURN p.page_name AS name, connections 
ORDER BY connections DESC
LIMIT 5;

MATCH (p:Company)-[:CONNECTED]->(:Page)
WITH COUNT(*) AS connections, p
RETURN p.page_name AS name, connections 
ORDER BY connections DESC
LIMIT 5;

// Shortest Path
MATCH path=shortestPath((:Company {page_name:'Facebook'})-[*0..10]-(:Page {page_name:'KLM Royal Dutch Airlines'}))
RETURN min(path)

MATCH path=shortestPath((:Company {page_name:'Facebook'})<-[*0..10]-(:Government {page_name:'U.S. Army Aviation and Missile Command'}))
RETURN min(path)


//Centrality, Betweness, Community Detection
