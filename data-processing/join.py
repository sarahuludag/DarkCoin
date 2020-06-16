from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, concat_ws, udf, concat, col, lit, when
from pyspark.sql.types import *
import cassandra
from datetime import datetime
from dateutil.relativedelta import relativedelta
from pyspark.sql import *


# Write result data frame into PostgreSQL 
def write_postgres(result):
	mode = "append"
	url = "jdbc:postgresql://" + $POSTGRESHOST + $POSTGRESPORT + $POSTGRESDB
	properties = {"user": $POSTGRESUSER,"password": $POSTGRESPASS,"driver": "org.postgresql.Driver"}
	result.write.jdbc(url=url, table=$POSTGRESTRANSACTIONS, mode=mode, properties=properties)


# Main 
appName = "DarkCoinJoin"
master = $MASTERIP

# Create Spark session
spark = SparkSession.builder \
    .appName(appName) \
    .config("spark.cassandra.connection.host", $CASSANDRAHOSTS) \
  	.config('spark.cassandra.connection.port', $CASSANDRAPORT) \
  	.config('spark.cassandra.output.consistency.level','ONE') \
  	.config('spark.kryoserializer.buffer.max','2047m') \
  	.config('spark.driver.port', $DRIVERPORT) \
    .getOrCreate()

sqlContext = SQLContext(spark)

# Read Bitcoin table from Cassandra
df_bitcoin = spark.read.format("org.apache.spark.sql.cassandra") \
	.options(table=$CASSANDRABITCOIN, keyspace=$CASSANDRAKEYSPACE) \
	.load()
df_bitcoin.registerTempTable("bitcoin")

# Read Marketplace Table from Cassandra
df_marketplace = spark.read.format("org.apache.spark.sql.cassandra") \
	.options(table=$CASSANDRAMARKETS, keyspace=$CASSANDRAKEYSPACE) \
	.load() 
df_marketplace.registerTempTable("marketplace")

# Udf function to increment and return date num_days
date_incr = (udf(lambda date, num_days:  date + relativedelta(days=num_days), TimestampType()))

# Join
result = df_bitcoin \
	.join(df_marketplace, (df_bitcoin.recv_amount == df_marketplace.price) & \
		(df_bitcoin.time > df_marketplace.ad_date) & (df_bitcoin.time <  date_incr(df_marketplace.ad_date, lit(10))), 'inner') \
	.drop(df_bitcoin.recv_amount) 

# Show size of result with a sample 
result.show(20,False)
print("------------ Total: " + str(result.count()))

# Write into Postgres
write_postgres(result)









