
// Customer Dataset to Customer, City and State Node
:auto USING PERIODIC COMMIT 100
LOAD CSV WITH HEADERS FROM 'https://raw.githubusercontent.com/BiaPri/Graph_Field/master/data_e_commerce/customers.csv' AS row
MERGE (c:Customer {id: toInteger(row.customer_id), name: row.customer_name, gender: row.gender, age: toInteger(row.age)})
MERGE (ct:City {city_name: row.city, zip_code: toInteger(row.zip_code), address: row.home_address})
MERGE (s:State {name: row.state})
MERGE (c)-[:LIVING_CITY]->(ct)
MERGE (c)-[:LIVING_STATE]->(s)

// Product Datset to Product node
LOAD CSV WITH HEADERS FROM 'https://raw.githubusercontent.com/BiaPri/Graph_Field/master/data_e_commerce/products.csv' AS row
MERGE (:Product {id: toInteger(row.prodct_ID), type: row.product_type, name: row.product_name, 
                 size: row.size, color: row.color, price: toInteger(row.price), stock: toInteger(row.quantity)})

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

// Displaying
MATCH (n)
RETURN n LIMIT 10;

// Delete ALL
MATCH (n)
DETACH DELETE n;
