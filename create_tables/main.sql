CREATE TABLE customer_sales (
    id INTEGER,
    customer_first_name TEXT,
    customer_last_name TEXT,
    customer_age INTEGER,
    customer_email TEXT,
    customer_country TEXT,
    customer_postal_code TEXT,
    customer_pet_type TEXT,
    customer_pet_name TEXT,
    customer_pet_breed TEXT,
    seller_first_name TEXT,
    seller_last_name TEXT,
    seller_email TEXT,
    seller_country TEXT,
    seller_postal_code TEXT,
    product_name TEXT,
    product_category TEXT,
    product_price DECIMAL(10, 2),
    product_quantity INTEGER,
    sale_date DATE,
    sale_customer_id INTEGER,
    sale_seller_id INTEGER,
    sale_product_id INTEGER,
    sale_quantity INTEGER,
    sale_total_price DECIMAL(10, 2),
    store_name TEXT,
    store_location TEXT,
    store_city TEXT,
    store_state TEXT,
    store_country TEXT,
    store_phone TEXT,
    store_email TEXT,
    pet_category TEXT,
    product_weight DECIMAL(10, 2),
    product_color TEXT,
    product_size TEXT,
    product_brand TEXT,
    product_material TEXT,
    product_description TEXT,
    product_rating DECIMAL(3, 1),
    product_reviews INTEGER,
    product_release_date DATE,
    product_expiry_date DATE,
    supplier_name TEXT,
    supplier_contact TEXT,
    supplier_email TEXT,
    supplier_phone TEXT,
    supplier_address TEXT,
    supplier_city TEXT,
    supplier_country TEXT
);


-- Create a temporary table with the same structure as customer_sales
CREATE TEMP TABLE temp_customer_sales (LIKE customer_sales INCLUDING ALL);

DO $$
DECLARE
    i INT;
BEGIN
    FOR i IN 0..9 LOOP
        -- Clear temp table for the new file
        TRUNCATE temp_customer_sales;

        -- Load data from the CSV file into the temp table
        EXECUTE format(
            'COPY temp_customer_sales FROM %L CSV HEADER',
            format('/data/MOCK_DATA (%s).csv', i)
        );

        -- Offset the id fields by 1000 * i to avoid collisions
        EXECUTE format(
            'UPDATE temp_customer_sales
                SET id = id + %s,
                    sale_customer_id = sale_customer_id + %s,
                    sale_seller_id = sale_seller_id + %s,
                    sale_product_id = sale_product_id + %s',
            1000*i, 1000*i, 1000*i, 1000*i
        );

        -- Insert the transformed data into the main table
        EXECUTE 'INSERT INTO customer_sales SELECT * FROM temp_customer_sales';
    END LOOP;
END
$$ LANGUAGE plpgsql;


---------------------------------------------------------------------------------------

CREATE TABLE dim_customer (
    customer_id INT PRIMARY KEY,
    first_name VARCHAR(50),
    last_name VARCHAR(50),
    age INT,
    email VARCHAR(50),
    country VARCHAR(50),
    postal_code VARCHAR(50)
);

INSERT INTO dim_customer
SELECT DISTINCT sale_customer_id AS customer_id,
       customer_first_name AS first_name,
       customer_last_name AS last_name,
       customer_age AS age,
       customer_email AS email,
       customer_country AS country,
       customer_postal_code AS postal_code
FROM customer_sales;

CREATE TABLE dim_customer_pet (
    pet_id SERIAL PRIMARY KEY,
    customer_id INT REFERENCES dim_customer(customer_id),
    name VARCHAR(50),
    type VARCHAR(50),
    breed VARCHAR(50),
    category VARCHAR(50)
);

INSERT INTO dim_customer_pet (customer_id, name, type, breed, category)
SELECT DISTINCT sale_customer_id AS customer_id,
       customer_pet_name AS name,
       customer_pet_type AS type,
       customer_pet_breed AS breed,
       pet_category AS category
FROM customer_sales
WHERE customer_pet_name IS NOT NULL;

CREATE TABLE dim_seller (
    seller_id INT PRIMARY KEY,
    first_name VARCHAR(50),
    last_name VARCHAR(50),
    email VARCHAR(50),
    country VARCHAR(50),
    postal_code VARCHAR(50)
);

INSERT INTO dim_seller
SELECT DISTINCT sale_seller_id AS seller_id,
       seller_first_name AS first_name,
       seller_last_name AS last_name,
       seller_email AS email,
       seller_country AS country,
       seller_postal_code AS postal_code
FROM customer_sales;

CREATE TABLE dim_product (
    product_id INT PRIMARY KEY,
    name VARCHAR(50),
    category VARCHAR(50),
    price FLOAT,
    quantity INT,
    weight FLOAT,
    color VARCHAR(50),
    size VARCHAR(50),
    brand VARCHAR(50),
    material VARCHAR(50),
    description VARCHAR(1024),
    rating FLOAT,
    reviews INT,
    release_date DATE,
    expiry_date DATE
);

INSERT INTO dim_product
SELECT DISTINCT sale_product_id AS product_id,
       product_name AS name,
       product_category AS category,
       product_price AS price,
       product_quantity AS quantity,
       product_weight AS weight,
       product_color AS color,
       product_size AS size,
       product_brand AS brand,
       product_material AS material,
       product_description AS description,
       product_rating AS rating,
       product_reviews AS reviews,
       product_release_date::DATE AS release_date,
       product_expiry_date::DATE AS expiry_date
FROM customer_sales;

CREATE TABLE dim_supplier (
    supplier_id SERIAL PRIMARY KEY,
    name VARCHAR(50),
    contact VARCHAR(50),
    email VARCHAR(50),
    phone VARCHAR(50),
    address VARCHAR(50),
    city VARCHAR(50),
    country VARCHAR(50)
);

INSERT INTO dim_supplier (name, contact, email, phone, address, city, country)
SELECT DISTINCT supplier_name AS name,
       supplier_contact AS contact,
       supplier_email AS email,
       supplier_phone AS phone,
       supplier_address AS address,
       supplier_city AS city,
       supplier_country AS country
FROM customer_sales
WHERE supplier_name IS NOT NULL;

CREATE TABLE dim_store (
    store_id SERIAL PRIMARY KEY,
    name VARCHAR(50),
    location VARCHAR(50),
    city VARCHAR(50),
    state VARCHAR(50),
    country VARCHAR(50),
    phone VARCHAR(50),
    email VARCHAR(50)
);

INSERT INTO dim_store (name, location, city, state, country, phone, email)
SELECT DISTINCT store_name AS name,
       store_location AS location,
       store_city AS city,
       store_state AS state,
       store_country AS country,
       store_phone AS phone,
       store_email AS email
FROM customer_sales
WHERE store_name IS NOT NULL;

CREATE TABLE dim_date (
    date_id SERIAL PRIMARY KEY,
    date DATE,
    day INT,
    month INT,
    year INT,
    weekday VARCHAR(15)
);

INSERT INTO dim_date (date, day, month, year, weekday)
SELECT DISTINCT sale_date::DATE AS date,
       EXTRACT(DAY FROM sale_date::DATE) AS day,
       EXTRACT(MONTH FROM sale_date::DATE) AS month,
       EXTRACT(YEAR FROM sale_date::DATE) AS year,
       TO_CHAR(sale_date::DATE, 'Day') AS weekday
FROM customer_sales;

CREATE TABLE fact_sales (
    sale_id INT,
    customer_id INT REFERENCES dim_customer(customer_id),
    seller_id INT REFERENCES dim_seller(seller_id),
    product_id INT REFERENCES dim_product(product_id),
    supplier_id INT REFERENCES dim_supplier(supplier_id),
    store_id INT REFERENCES dim_store(store_id),
    date_id INT REFERENCES dim_date(date_id),
    quantity INT,
    total_price FLOAT
);

INSERT INTO fact_sales
SELECT c.id AS sale_id,
       c.sale_customer_id AS customer_id,
       c.sale_seller_id AS seller_id,
       c.sale_product_id AS product_id,
       sup.supplier_id,
       st.store_id,
       d.date_id,
       c.sale_quantity AS quantity,
       c.sale_total_price AS total_price
FROM customer_sales c
LEFT JOIN dim_supplier sup ON c.supplier_name = sup.name
                          AND c.supplier_contact = sup.contact
                          AND c.supplier_email = sup.email
                          AND c.supplier_phone = sup.phone
                          AND c.supplier_address = sup.address
                          AND c.supplier_city = sup.city
                          AND c.supplier_country = sup.country
LEFT JOIN dim_store st ON c.store_name = st.name
                      AND c.store_location = st.location
                      AND c.store_city = st.city
                      AND c.store_state = st.state
                      AND c.store_country = st.country
                      AND c.store_phone = st.phone
                      AND c.store_email = st.email
JOIN dim_date d ON c.sale_date::DATE = d.date;
