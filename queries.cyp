
// Customer Dataset to Customer, City and State Node
:auto USING PERIODIC COMMIT 100
LOAD CSV WITH HEADERS FROM 'https://raw.githubusercontent.com/BiaPri/Graph_Field/master/data_e_commerce/customers.csv' AS row
MERGE (c:Customer {id: toInteger(row.customer_id), name: row.customer_name, gender: row.gender, age: toInteger(row.age)})
MERGE (ct:City {city_name: row.city, zip_code: toInteger(row.zip_code), address: row.home_address})
MERGE (s:State {name: row.state})
MERGE (c)-[:LIVING_CITY]->(ct)
MERGE (c)-[:LIVING_STATE]->(s)

// Faster Using APOC (select right batchSize)
CALL apoc.periodic.iterate(
"CALL apoc.load.csv('https://raw.githubusercontent.com/BiaPri/Graph_Field/master/data_e_commerce/customers.csv')
 YIELD map AS row RETURN row",
"MERGE (c:Customer {id: toInteger(row.customer_id), name: row.customer_name, gender: row.gender, age: toInteger(row.age)})
 MERGE (ct:City {city_name: row.city, zip_code: toInteger(row.zip_code), address: row.home_address})
 MERGE (s:State {name: row.state})
 MERGE (c)-[:LIVING_CITY]->(ct)
 MERGE (c)-[:LIVING_STATE]->(s)",
 {batchSize: 150}
)

// Product Datset to Product node
LOAD CSV WITH HEADERS FROM 'https://raw.githubusercontent.com/BiaPri/Graph_Field/master/data_e_commerce/products.csv' AS row
MERGE (:Product {id: toInteger(row.product_ID), type: row.product_type, name: row.product_name, 
                 size: row.size, color: row.colour, price: toInteger(row.price), stock: toInteger(row.quantity)})

// Creating Relationship between Customer and Product Node
LOAD CSV WITH HEADERS FROM 'https://raw.githubusercontent.com/BiaPri/Graph_Field/master/data_e_commerce/orders_sales.csv' AS row
MATCH (c:Customer {id: toInteger(row.customer_id)}) 
MATCH (p:Product {id: toInteger(row.product_id)})
MERGE (c)-[:ORDERS {order_date: date(row.order_date), delivery_date: date(row.delivery_date), quantity: toInteger(row.quantity)}]->(p)

// Finding Customers living in the South Australia
MATCH (c:Customer)-[:LIVING_STATE]->(s:State)
WHERE s.name = 'South Australia'
RETURN count(*) AS num_customers;

// Finding Customers living in two states
UNWIND ['South Australia', 'Queensland'] AS st
MATCH (:Customer)-[:LIVING_STATE]->(s:State)
WHERE s.name = st
RETURN s.name AS state, count(*) AS num_customers;

// Counting number of Customers living in the same city
MATCH (c:Customer)-[:LIVING_CITY]->(ct:City)
WITH ct.city_name AS city, count(c.name) AS num_customers
RETURN city, num_customers 
ORDER BY num_customers DESC;

// Counting number of Customers living in the same zipcode
MATCH (c:Customer)-[:LIVING_CITY]->(ct:City)
WITH ct.zip_code AS zip_code, count(c.name) AS num_customers
RETURN zip_code, num_customers 
ORDER BY num_customers DESC;


// Creating graph projection of Customers and Products
CALL gds.graph.create('e-Commerce', ['Customer', 'Product'],
					  {
                        ORDERS: {
                                  type: 'ORDERS',
                                  orientation: 'NATURAL',
                                  properties: 'quantity'
                                 }
                           }
                       );

// Dropping graph
CALL gds.graph.drop('e-Commerce')

// Node Similarity (weighted) Stream
CALL gds.nodeSimilarity.stream('e-Commerce', {relationshipWeightProperty: 'quantity', similarityCutoff: 0.1})
YIELD node1, node2, similarity
RETURN gds.util.asNode(node1).name AS Customer1,
       gds.util.asNode(node2).name AS Customer2,
       similarity
ORDER BY similarity DESC;

// Node Similarity write (create relationship)
CALL gds.nodeSimilarity.write('e-Commerce', 
    {
        writeRelationshipType: 'SIMILAR', 
        writeProperty: 'score',
        relationshipWeightProperty: 'quantity', 
        similarityCutoff: 0.1
    }
)
YIELD nodesCompared, relationshipsWritten

// Graph projection of SIMILAR Customers
CALL gds.graph.create('Sim_Customers', 'Customer', 
                        {
                            SIMILAR: {
                                    type: 'SIMILAR',
                                    orientation: 'UNDIRECTED'
                                    }
                        }
                )  

// WCC Algo => Community detection
CALL gds.wcc.write('Sim_Customers', { writeProperty: 'componentId' })
YIELD nodePropertiesWritten, componentCount;

// Louvain Algo
CALL gds.louvain.write('Sim_Customers', { writeProperty: 'community' })
YIELD  communityCount, modularity, modularities;



// Displaying
MATCH (n)
RETURN n LIMIT 10;

// Delete ALL
MATCH (n)
DETACH DELETE n;
