LOAD CSV WITH HEADERS FROM 'https://raw.githubusercontent.com/BiaPri/Graph_Field/master/data_e_commerce/customers.csv' AS row
MERGE (:Customer {id: row.customer_id, name: row.customer_name, gender: row.gender, age:row.age})
MERGE (:City {city_name: row.city, zip_code: row.zip_code, address: row.home_address});


// Displaying
MATCH (n)
RETURN n LIMIT 10;

// Delete ALL
MATCH (n)
DETACH DELETE n;
