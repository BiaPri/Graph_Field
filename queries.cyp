// To ZipCode, State, City + CSV and PERSONA IDENTIFICATION + age range

// Constraints
CREATE CONSTRAINT ON (c:Customer) ASSERT c.id IS UNIQUE;
CREATE CONSTRAINT ON (p:Product) ASSERT p.id IS UNIQUE;
CREATE CONSTRAINT ON (ps:ProductName) ASSERT ps.name IS UNIQUE;

// Show Schema 
CALL db.schema.visualization()

// Customer Dataset to Customer
:auto USING PERIODIC COMMIT 100
LOAD CSV WITH HEADERS FROM 'https://raw.githubusercontent.com/BiaPri/Graph_Field/master/data_e_commerce/customers.csv' AS row
MERGE (c:Customer {id: toInteger(row.customer_id), name: row.customer_name, gender: row.gender, age: toInteger(row.age), state: row.state})

// Faster using APOC (select right batchSize)
CALL apoc.periodic.iterate(
"CALL apoc.load.csv('https://raw.githubusercontent.com/BiaPri/Graph_Field/master/data_e_commerce/customers.csv')
 YIELD map AS row RETURN row",
"MERGE (c:Customer {id: toInteger(row.customer_id), name: row.customer_name, gender: row.gender, age: toInteger(row.age), state: row.state})",
 {batchSize: 200}
)

// Product Datset to Product node
:auto USING PERIODIC COMMIT 100
LOAD CSV WITH HEADERS FROM 'https://raw.githubusercontent.com/BiaPri/Graph_Field/master/data_e_commerce/products.csv' AS row
MERGE (:Product {id: toInteger(row.product_ID), type: row.product_type, name: row.product_name, 
                 size: row.size, color: row.colour, price: toInteger(row.price)})

// Faster using APOC (select right batchSize)
CALL apoc.periodic.iterate(
"CALL apoc.load.csv('https://raw.githubusercontent.com/BiaPri/Graph_Field/master/data_e_commerce/products.csv')
 YIELD map AS row RETURN row",
"MERGE (:Product {id: toInteger(row.product_ID), type: row.product_type, name: row.product_name, 
                 size: row.size, color: row.colour, price: toInteger(row.price)})",
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

// Finding Customers living in the South Australia
MATCH (c:Customer)
WHERE c.state = 'South Australia'
RETURN c.state AS state, count(*) AS num_customers;

// Finding Customers living in two states
UNWIND ['South Australia', 'Queensland'] AS st
MATCH (:Customer)
WHERE s.name = st
RETURN s.name AS state, count(*) AS num_customers;

MATCH (c:Customer)
WHERE c.state IN ['South Australia', 'Queensland']
RETURN c.state AS state, count(*) AS num_customers;

// Finding #Customer living in different states
MATCH (c:Customer)
RETURN c.state AS state, count(*) AS num_customers
ORDER BY num_customers DESC;

// Gender Distribution
MATCH (c:Customer)
RETURN c.gender AS gender, count(*) AS num_customers
ORDER BY num_customers DESC;

// Product Name Distribution
MATCH (p:Product)
RETURN p.name AS product_name, count(*) AS num_products
ORDER BY num_products DESC;

// Number of order per customers
MATCH (c:Customer)-[r:ORDERS]->(:Product)
RETURN c.name AS customer_name, count(r) AS orders
ORDER BY orders DESC;

// Number of quantity ordered per customers
MATCH (c:Customer)-[r:ORDERS]->(:Product)
RETURN c.name AS customer_name, sum(r.quantity) AS quantity
ORDER BY orders DESC;

MATCH (c:Customer)-[r:ORDERS]->(p:Product)
RETURN c.name AS customer_name, sum(p.price*r.quantity) AS spend, sum(r.quantity) AS quantity, count(r) AS orders 
ORDER BY total_price DESC
LIMIT 5;

// Customers that bought more than "n" products
MATCH (c:Customer)-[r:ORDERS]->(p:Product)
WITH c.name AS customer, sum(r.quantity) AS bought_products
WHERE bought_products >= 5
RETURN customer, bought_products
ORDER BY bought_products DESC;

// Customers that did not order any product
MATCH (c:Customer)
WHERE NOT (c)-[:ORDERS]-(:Product)
RETURN count(c) AS do_not_order

// Product not ordered by anyone 
MATCH (p:Product)
WHERE NOT (p)-[:ORDERS]-(:Customer)
RETURN count(p) AS are_not_ordered


// APOC Testing
MATCH (c:Customer)-[r:ORDERS]->(p:Product)
RETURN apoc.agg.statistics(c.age) AS age_distribution,  
       apoc.agg.statistics(p.price) AS price_distribution,
       apoc.agg.statistics(r.quantity) AS quantity_order_distribution;


// TOP 5 Best Selling Product 
MATCH (c:Customer)-[r:ORDERS]->(p:Product)
RETURN p, sum(r.quantity) AS num_sales
ORDER BY num_sales DESC
LIMIT 5;

MATCH (c:Customer)-[r:ORDERS]->(p:Product)
RETURN p.name, sum(r.quantity) AS num_sales
ORDER BY num_sales DESC
LIMIT 5;

MATCH (c:Customer)-[r:ORDERS]->(p:Product)
RETURN p.color, sum(r.quantity) AS num_sales
ORDER BY num_sales DESC
LIMIT 5;

MATCH (c:Customer)-[r:ORDERS]->(p:Product)
RETURN p.size, sum(r.quantity) AS num_sales
ORDER BY num_sales DESC
LIMIT 5;

// Best Selling Date
MATCH (c:)


// TOP Buyer TOP 3 Products (COMPLEX)
CALL{
        MATCH (c:Customer)-[r:ORDERS]->(p:Product)
        WITH c.name as top_buyer, sum(r.quantity*p.price) as spend
        RETURN top_buyer
        ORDER BY spend DESC
        LIMIT 1
    }
WITH top_buyer
CALL{
        WITH top_buyer
        MATCH (c:Customer{name:top_buyer})-[r:ORDERS]->(p:Product)
        WITH p.name AS product, sum(r.quantity) AS num_sales
        RETURN product
        ORDER BY num_sales DESC
        LIMIT 3
   }
WITH top_buyer, collect(product) AS products
RETURN top_buyer, products



RETURN top_buyer

ORDER BY spend DESC
LIMIT 1;

// Best Selling Date TOP Buyer TOP 2 Products


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
        similarityCutoff: 0.4
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

// Weakly connected components (Anonymous Graph)
CALL gds.wcc.stream(
                        {
                            nodeProjection: 'Customer',
                            relationshipProjection: 'SIMILAR',
                            nodeProperties: ['age', 'ohe_gender']
                        }
                    )
YIELD nodeId, componentId
WITH componentId, count(nodeId) AS num_customers
RETURN componentId, num_customers
ORDER BY num_customers DESC

CALL gds.wcc.write('Segmentation', { writeProperty: 'wccId'})
YIELD nodePropertiesWritten, componentCount;

// Louvain => Community detection (Cypher Graph)
CALL gds.graph.drop('Segmentation');

CALL gds.graph.create.cypher(
                             'Segmentation',
                             'MATCH (c:Customer)
                              WHERE c.wccId = 1
                              RETURN id(c) AS id, c.age AS age, c.ohe_gender AS gender',
                             'MATCH (c1)-[r:SIMILAR]->(c2)
                              WHERE c1.wccId = 1 AND c2.wccId = 1 
                              RETURN id(c1) AS source, id(c2) AS target, type(r) AS type, r.score AS score'
                            ) 
                        YIELD graphName AS graph, nodeCount AS nodes, relationshipCount AS rels

CALL gds.louvain.stream('Segmentation',
                        {relationshipWeightProperty: 'score', includeIntermediateCommunities: true})
YIELD  nodeId, communityId, intermediateCommunityIds;

// Coloring Communities [140, 566, 578, 440, 360, 394, 416, 432, 594, 494, 327, 396, 299, 251]
CALL gds.louvain.write('Segmentation',
                        {relationshipWeightProperty: 'score', includeIntermediateCommunities: false, writeProperty: 'community'})
YIELD communityCount, modularity, modularities


// Export to csv use python driver (py2neo) --> Persona + ML?


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