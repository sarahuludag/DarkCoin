from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, concat_ws, udf, concat, col, lit, when
from pyspark.sql.types import *
import cassandra
from datetime import datetime
from pyspark.sql import *



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

# Read Bitcoin data from Cassandra
df_bitcoin = spark.read.format("org.apache.spark.sql.cassandra") \
	.options(table=$CASSANDRABITCOIN, keyspace=$CASSANDRAKEYSPACE) \
	.load()
df_bitcoin.show()

# Read Silkroad data from Cassandra
df_darkweb = spark.read.format("org.apache.spark.sql.cassandra") \
	.options(table=$CASSANDRASILKROAD, keyspace=$CASSANDRAKEYSPACE) \
	.load() \
	.filter("ad_date='2014-01-16 00:00:00'")
df_darkweb.show()

print("-----------Pre processing-----------------")
result = df_bitcoin.join(df_darkweb, df_bitcoin.recv_amount == df_darkweb.price, 'inner').drop(df_bitcoin.recv_amount).collect()

# Convert list to RDD
rdd = spark.sparkContext.parallelize(result)

# Create a schema for the dataframe	
schema = StructType([
	StructField('tx_hash', StringType(), True),
	StructField('send_addr', StringType(), True),
	StructField('recv_addr', StringType(), True),
	StructField('time', DateType(), True),
	StructField('block_num', StringType(), True),
	StructField('send_amount', FloatType(), True),
	StructField('price', FloatType(), True),
	StructField('product_name', StringType(), True),
	StructField('ad_date', DateType(), True),
	StructField('category', StringType(), True),
	StructField('href', StringType(), True),
	StructField('vendor', StringType(), True),
	StructField('ship_from', StringType(), True),
	StructField('ship_to', StringType(), True)
])

# Create data frame
df = spark.createDataFrame(rdd,schema)
df.show(20,False)

