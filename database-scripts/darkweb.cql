

----------------CASSANDRA CQL-------------------

CREATE KEYSPACE darkweb WITH replication = {'class': 'SimpleStrategy', 'replication_factor' : 2};

USE darkweb;

---------DARKWEBMARKETPLACE--------

DROP TABLE marketplace;

CREATE TABLE marketplace (
	marketplace text,
	product_name text, 
	price float,
	category text, 
	vendor text,
	description text,
	ad_date timestamp, 
	ship_to text, 
	ship_from text,
	image_url text,
	PRIMARY KEY ((price, product_name), ad_date), 
) WITH CLUSTERING ORDER BY (ad_date DESC);


---------BITCOIN----------

DROP TABLE bitcoin;

CREATE TABLE bitcoin (
	tx_hash text,
	send_addr text, 
	send_amount float,
	recv_addr text, 
	recv_amount float, 
	time timestamp, 
	block_num text,
	PRIMARY KEY ((tx_hash, send_addr, recv_addr), time, recv_amount), 
) WITH CLUSTERING ORDER BY (time DESC, recv_amount DESC);

















