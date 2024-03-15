USE DATABASE DB01

CREATE TABLE Transactions (
    CLIENT_ID BIGINT,
    CLIENT_NAME VARCHAR,
    CLIENT_LASTNAME VARCHAR,
    EMAIL VARCHAR,
    STORE_ID BIGINT,
    STORE_NAME VARCHAR,
    LOCATION VARCHAR,
    PRODUCT_ID BIGINT,
    PRODUCT_NAME VARCHAR,
    CATEGORY VARCHAR,
    BRAND VARCHAR,
    ADDRESS_ID BIGINT,
    STREET VARCHAR,
    CITY VARCHAR,
    STATE VARCHAR,
    ZIP_CODE VARCHAR,
    TRANSACTION_ID BIGINT,
    QUANTITY_OF_ITEMS_SOLD INT,
    UNIT_PRICE FLOAT,
    DISCOUNT FLOAT
);

select* from TRANSACTIONS 


// ******************* Create_Dimension_tables*******************

// Create Client_Table
CREATE TABLE Client (
    CLIENT_ID BIGINT PRIMARY KEY,
    CLIENT_NAME VARCHAR,
    CLIENT_LASTNAME VARCHAR,
    EMAIL VARCHAR
);

ALTER TABLE Transactions 
ADD FOREIGN KEY (CLIENT_ID) REFERENCES Client(CLIENT_ID);

//Create Store_Table
CREATE TABLE Store(
 STORE_ID BIGINT PRIMARY KEY ,
 STORE_NAME VARCHAR,
 LOCATION VARCHAR
 
);
ALTER TABLE Transactions 
ADD FOREIGN KEY (STORE_ID) REFERENCES Store(STORE_ID);

// Create Product_Table
CREATE TABLE Product (
    PRODUCT_ID BIGINT PRIMARY KEY ,
    PRODUCT_NAME VARCHAR,
    CATEGORY VARCHAR,
    BRAND VARCHAR
    
);
ALTER TABLE Transactions 
ADD FOREIGN KEY (PRODUCT_ID) REFERENCES Product(PRODUCT_ID);

//create a Address_table
CREATE TABLE Address (
  ADDRESS_ID BIGINT PRIMARY KEY,
    STREET VARCHAR,
    CITY VARCHAR,
    STATE VARCHAR,
    ZIP_CODE VARCHAR
);
ALTER TABLE Transactions 
ADD FOREIGN KEY (ADDRESS_ID) REFERENCES Address(ADDRESS_ID);



CREATE TABLE Sales (
    transaction_id BIGINT,
    quantity_of_items_sold INT,
    unit_price DECIMAL(18, 2),
    discount DECIMAL(18, 2),
    client_id BIGINT, 
    product_id BIGINT,
    store_id BIGINT, 
    address_id BIGINT, 
    PRIMARY KEY (transaction_id),
    FOREIGN KEY (client_id) REFERENCES Client(CLIENT_ID),
    FOREIGN KEY (product_id) REFERENCES Product(PRODUCT_ID),
    FOREIGN KEY (store_id) REFERENCES Store(STORE_ID),
    FOREIGN KEY (address_id) REFERENCES Address(ADDRESS_ID)
);

// fill Info for calculations

INSERT INTO Sales (transaction_id, quantity_of_items_sold, unit_price, discount, client_id, product_id, store_id, address_id)
SELECT transaction_id, quantity_of_items_sold, unit_price, discount, client_id, product_id, store_id, address_id
FROM Transactions;


INSERT INTO Product (product_id, product_name, category, brand)
SELECT DISTINCT product_id, product_name, category, brand
FROM Transactions;

INSERT INTO STORE (store_id, store_name,location)
SELECT DISTINCT store_id, store_name, location
FROM Transactions;




// key metrics calculation

//Average Order Value
SELECT AVG(unit_price * quantity_of_items_sold) AS average_order_value
FROM Sales;


// Sales by Product Category

SELECT p.category, SUM(s.unit_price * s.quantity_of_items_sold) AS total_sales_by_category
FROM Sales s
JOIN Product p ON s.product_id = p.product_id
GROUP BY p.category;

//Sales by Store/Location
SELECT st.location, SUM(s.unit_price * s.quantity_of_items_sold) AS total_sales_by_location
FROM Sales s
JOIN Store st ON s.store_id = st.store_id
GROUP BY st.location;
