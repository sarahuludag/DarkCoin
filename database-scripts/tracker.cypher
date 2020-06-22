
-------Create DB------

CREATE DATABASE coinTracker

-------Delete All----
MATCH (n)
DETACH DELETE n

------Create Constraints-----

CREATE CONSTRAINT tx_unique
ON (tx:Transaction) ASSERT tx.hash IS UNIQUE;

CREATE CONSTRAINT pkey_unique
ON (key:PublicKey) ASSERT key.pkey IS UNIQUE;


------Import Bitcoin Transactions From Json File In S3-----
CALL apoc.load.json($SIGNEDURL)
YIELD value
WITH value LIMIT 1000
MERGE (tx:Transaction {hash: value.tx_hash})
MERGE (sender:PublicKey {pkey: value.send_addr})
MERGE (sender)-[:SEND]->(tx)
MERGE (receiver:PublicKey {pkey: value.recv_addr})
MERGE (tx)-[:RECEIVE]->(receiver);


---------Iterative Import---------
CALL apoc.periodic.iterate(
  "CALL apoc.load.json($SIGNEDURL)
  YIELD value",
  "MERGE (tx:Transaction {hash: value.tx_hash})
	MERGE (sender:PublicKey {pkey: value.send_addr})
	MERGE (sender)-[:SEND]->(tx)
	MERGE (receiver:PublicKey {pkey: value.recv_addr})
	MERGE (tx)-[:RECEIVE]->(receiver)",
{batchSize: 10});


-------Query Transactions-----
MATCH p=()-[:SEND]-()-[:RECEIVE]-()
RETURN p

-----Get number of public keys------
MATCH (n:PublicKey)
RETURN count(n) as count


------Find incoming transactions of a specific public key
MATCH (otherNodes)-[:SEND]-(otherTransactions)-[:RECEIVE]-(n:PublicKey{ pkey: '1MwpmkSkbaD9QVeEz5Mfj14njsuz9rpE3t' })
RETURN n, otherNodes, otherTransactions




