// Constraints
CREATE CONSTRAINT ON (c:Customer) ASSERT c.id IS UNIQUE;
CREATE CONSTRAINT ON (p:Product) ASSERT p.id IS UNIQUE;
CREATE CONSTRAINT ON (c:City) ASSERT c.name IS UNIQUE;
CREATE CONSTRAINT ON (ps:ProductName) ASSERT ps.name IS UNIQUE;

// Show Schema 
CALL db.schema.visualization()

// Customer Dataset to Customer, City and State Node 
:auto USING PERIODIC COMMIT 100
LOAD CSV WITH HEADERS FROM 'https://raw.githubusercontent.com/BiaPri/Graph_Field/master/data_e_commerce/customers.csv' AS row
MERGE (c:Customer {id: toInteger(row.customer_id), name: row.customer_name, gender: row.gender, age: toInteger(row.age)})
MERGE (ct:City {name: row.city})
MERGE (s:State {name: row.state})
MERGE (c)-[:LIVING_CITY]->(ct)
MERGE (c)-[:LIVING_STATE]->(s)

// Faster using APOC (select right batchSize)
CALL apoc.periodic.iterate(
"CALL apoc.load.csv('https://raw.githubusercontent.com/BiaPri/Graph_Field/master/data_e_commerce/customers.csv')
 YIELD map AS row RETURN row",
"MERGE (c:Customer {id: toInteger(row.customer_id), name: row.customer_name, gender: row.gender, age: toInteger(row.age)})
 MERGE (ct:City {name: row.city})
 MERGE (s:State {name: row.state})
 MERGE (c)-[:LIVING_CITY]->(ct)
 MERGE (c)-[:LIVING_STATE]->(s)",
 {batchSize: 150}
)

// Product Datset to Product node
:auto USING PERIODIC COMMIT 100
LOAD CSV WITH HEADERS FROM 'https://raw.githubusercontent.com/BiaPri/Graph_Field/master/data_e_commerce/products.csv' AS row
MERGE (:Product {id: toInteger(row.product_ID), type: row.product_type, name: row.product_name, 
                 size: row.size, color: row.colour, price: toInteger(row.price), stock: toInteger(row.quantity)})

// Faster using APOC (select right batchSize)
CALL apoc.periodic.iterate(
"CALL apoc.load.csv('https://raw.githubusercontent.com/BiaPri/Graph_Field/master/data_e_commerce/products.csv')
 YIELD map AS row RETURN row",
"MERGE (:Product {id: toInteger(row.product_ID), type: row.product_type, name: row.product_name, 
                 size: row.size, color: row.colour, price: toInteger(row.price), stock: toInteger(row.quantity)})",
 {batchSize: 200}
)

// Creating Relationship between Customer and Product Node
LOAD CSV WITH HEADERS FROM 'https://raw.githubusercontent.com/BiaPri/Graph_Field/master/data_e_commerce/orders_sales.csv' AS row
MATCH (c:Customer {id: toInteger(row.customer_id)}) 
MATCH (p:Product {id: toInteger(row.product_id)})
MERGE (c)-[:ORDERS {order_date: date(row.order_date), delivery_date: date(row.delivery_date), quantity: toInteger(row.quantity)}]->(p)

// Faster using APOC (select right batchSize)
CALL apoc.periodic.iterate(
"CALL apoc.load.csv('https://raw.githubusercontent.com/BiaPri/Graph_Field/master/data_e_commerce/orders_sales.csv')
 YIELD map AS row RETURN row",
"MATCH (c:Customer {id: toInteger(row.customer_id)}) 
 MATCH (p:Product {id: toInteger(row.product_id)})
 MERGE (c)-[:ORDERS {order_date: date(row.order_date), delivery_date: date(row.delivery_date), quantity: toInteger(row.quantity)}]->(p)",
 {batchSize: 500}
)

// Sanity Check Products = 1260
MATCH (p:Product)
RETURN count(*) AS num_procucts

// Sanity Check Customers = 1000
MATCH (c:Customer)
RETURN count(*) AS num_customers

// Sanity Check Cities = 961
MATCH (c:City)
RETURN count(*) AS num_cities

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
WITH ct.name AS city, count(c.name) AS num_customers
RETURN city, num_customers 
ORDER BY num_customers DESC;

// Customers that bought more than "n" products
MATCH (c:Customer)-[r:ORDERS]->(p:Product)
WITH c.name AS customer, sum(r.quantity) AS bought_product
WHERE bought_product >= 5
RETURN customer, bought_product
ORDER BY bought_product DESC;

// Customers that did not order any product
MATCH (c:Customer)
WHERE NOT (c)-[:ORDERS]-(:Product)
RETURN count(c)


// NEW GRAPH 
// Reduce Complexity => Can Add color or size ++ quantity analysis
MATCH (c:Customer)-[r:ORDERS]->(p:Product)
WITH c, p, sum(r.quantity) AS quantity
MERGE (pn:ProductName {name: p.name})
MERGE (c)-[:BOUGHT {quantity: quantity}]->(pn)

// Sanity Check (Unique ProductName = 35)
MATCH (p:ProductName)
RETURN count(p.name) AS unique_names

// Sanity Check (Remaining Unique Customers = 616)
MATCH (c:Customer)-[:BOUGHT]->(p:ProductName)
RETURN count(DISTINCT c.name) AS num_customers

MATCH (c:Customer)-[:BOUGHT]->(p:ProductName)
DETACH DELETE c, p;

// TOP 5 Best Selling ProductName
MATCH (c:Customer)-[b:BOUGHT]->(p:ProductName)
RETURN p.name AS product_name, sum(b.quantity) AS bought_product
ORDER BY bought_product DESC
LIMIT 5;

// PREPROCESSING
// Gender to is a string => not accepted by gds One Hot Encoding? 8 Unique Genders
MATCH (c:Customer)
WITH collect(DISTINCT c.gender) AS genders
MATCH (c:Customer)
SET c.ohe_gender = gds.alpha.ml.oneHotEncoding(genders, [c.gender])


// Creating graph projection of Customers and Products
CALL gds.graph.create('e-Commerce',
                      {
                          Customer: {label: 'Customer', properties: ['age', 'ohe_gender']},
                          Product: {label: 'ProductName'}
                      },
					  {
                        BOUGHT: {
                                    type: 'BOUGHT',
                                    orientation: 'NATURAL',
                                    properties: 'quantity'
                                }
                           }
                       );

// Node Similarity Stream
CALL gds.nodeSimilarity.stream('e-Commerce', {similarityCutoff: 0.47})
YIELD node1, node2, similarity
RETURN gds.util.asNode(node1).name AS Customer1,
       gds.util.asNode(node2).name AS Customer2,
       similarity
ORDER BY similarity DESC;


// Node Similarity write (create relationship) [similarityCutoff ? 0.4 or 0.47]
CALL gds.nodeSimilarity.write('e-Commerce', 
    {
        writeRelationshipType: 'SIMILAR', 
        writeProperty: 'score',
        similarityCutoff: 0.47
    }
)
YIELD nodesCompared, relationshipsWritten

// Graph projection of Customers 
CALL gds.graph.create('Segmentation',
                        {
                          Customer: {label: 'Customer', properties: ['age', 'ohe_gender']}
                        },
                        {
                            SIMILAR: {
                                        type: 'SIMILAR',
                                        orientation: 'UNDIRECTED',
                                        properties: 'score'
                                     }
                        }
                )  

// Weakly connected components
CALL gds.wcc.stream('Segmentation')
YIELD nodeId, componentId
WITH componentId, count(nodeId) AS num_customers
RETURN componentId, num_customers
ORDER BY num_customers DESC

CALL gds.wcc.write('Segmentation', { writeProperty: 'wccId' })
YIELD nodePropertiesWritten, componentCount;

// Louvain => Community detection (Anonymous Graph)
CALL gds.louvain.stream({
                            {
                                Customer: {label: 'Customer', properties: ['age', 'ohe_gender', 'wccId']}
                            },
                            {
                                SIMILAR: {
                                            type: 'SIMILAR',
                                            orientation: 'UNDIRECTED',
                                            properties: 'score'
                                         }
                            }
                        },
                        {relationshipWeightProperty: 'score', writeProperty: 'louvainId'})
YIELD nodePropertiesWritten, componentCount;


// CLEAN UP DATABASE
// Delete ALL
MATCH (n)
DETACH DELETE n;

// Dropping graph
CALL gds.graph.drop('e-Commerce');
CALL gds.graph.drop('Segmentation');



// APPENDIX => ADDITIONAL RESSOURCES
// Node embedding
CALL gds.beta.node2vec.write('Graph_Name',
                                {
                                    embeddingDimension: 25,
                                    iterations: 10,
                                    walkLength: 10,
                                    writeProperty: "embeddingNode2vec"
                                }
                           );

// Creating graph projection using Cypher --> To complete --> Graph by gender (!)
CALL gds.graph.create.cypher('e-Commerce-Plus',
                             'MATCH (c:Customer)
                              MATCH (p:Product)
                              RETURN id(c), id(p)',
                             'MATCH (c:Customer)-[r:ORDERS]->(p:Product)
                              WITH c AS customer, r AS rel_order, p AS product, sum(r.quantity) AS quantity
                              WHERE quantity >= 5
                              RETURN id(customer) AS source, id(product) AS target, type(rel_order) AS type'
                            )