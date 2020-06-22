----------------SQL-------------------

-- Create DB
CREATE DATABASE insight;

-- List available DB
\l

-- Connect to a DB
\c insight;

-- List Tables
\dt

-- Drop Table 
DROP TABLE IF EXISTS transactions CASCADE;

-- Create Table
CREATE TABLE transactions(
   tx_id BIGSERIAL PRIMARY KEY NOT NULL,
   tx_hash VARCHAR(250) NOT NULL,
   send_addr VARCHAR(250) NOT NULL,
   recv_addr VARCHAR(250) NOT NULL,
   time DATE NOT NULL,
   block_num VARCHAR(50),
   send_amount NUMERIC NOT NULL,
   price NUMERIC NOT NULL,
   marketplace VARCHAR(100),
   product_name VARCHAR(511),
   ad_date DATE NOT NULL,
   category VARCHAR(100),
   image_url VARCHAR(511),
   vendor VARCHAR(50),
   description VARCHAR(511),
   ship_from VARCHAR(100),
   ship_to VARCHAR(100),
   UNIQUE(tx_hash, send_addr, recv_addr, product_name, price, ad_date)
);


---- QUERIES----
--Filter if there is a receiving address that has been flagged more than once for different products, also transactions have the same vendor
SELECT T1.* FROM transaction T1, transaction T2 WHERE T1.recv_addr = T2.recv_addr AND T1.tx_hash <> T2.tx_hash AND T1.vendor = T2.vendor AND T1.price <> T2.price AND T1.vendor IS NOT NULL AND T1.vendor <> '' GROUP BY T1.tx_id;

--Create new table from select query
CREATE TABLE acc_transac AS SELECT T1.* FROM transactions T1, transactions T2 WHERE T1.recv_addr = T2.recv_addr AND T1.tx_hash <> T2.tx_hash AND T1.vendor = T2.vendor AND T1.price <> T2.price AND T1.vendor IS NOT NULL AND T1.vendor <> '' GROUP BY T1.tx_id;


-- Example select distinct products for listing on the main page
SELECT DISTINCT(product_name), price, vendor, ad_date, category FROM acc_transac WHERE time >= '2014-01-20' AND time <= '2014-05-20' AND marketplace = 'agora' ORDER BY ad_date;

