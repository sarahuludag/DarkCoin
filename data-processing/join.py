

# Import libraries
from pyspark.sql import *
from pyspark.sql.types import *
from pyspark.sql import SparkSession
from pyspark import SparkContext, SparkConf
from pyspark.sql.functions import explode, concat_ws, udf, concat, col, lit, when

import cassandra
from datetime import datetime
from dateutil.relativedelta import relativedelta



# Write result data frame into PostgreSQL 
def write_postgres( result ):
	"""
	Write dataframe into PostgreSQL

	Parameters:
	result (dataframe): dataframe that stores the result of the join

	"""
	mode = "append"
	url = "jdbc:postgresql://" + config.postgres["HOST"] + config.postgres["PORT"] + config.postgres["DB"]
	properties = { "user": config.postgres["USER"], "password": config.postgres["PASS"], "driver": "org.postgresql.Driver" }
	result.write.jdbc( url = url, table = config.postgres["TRANSACTION"], mode = mode, properties = properties )


def main():
	"""
	Main function

	Main function that joins the bitcoin and dark web data

	"""

	# Create Spark session
	appName = "DarkCoinJoin"
	master = config.spark["MASTERIP"]

	spark = SparkSession.builder \
	    .appName( appName ) \
	    .config( "spark.cassandra.connection.host", config.cassandra["HOSTS"] ) \
	  	.config( 'spark.cassandra.connection.port', config.cassandra["PORT"] ) \
	  	.config( 'spark.cassandra.output.consistency.level', 'ONE' ) \
	  	.config( 'spark.kryoserializer.buffer.max', '2047m' ) \
	  	.config( 'spark.driver.port', config.cassandra["DRIVERPORT"] ) \
	  	.config( 'spark.network.timeout', '10000000' ) \
	  	.master( master ) \
	    .getOrCreate() 
	    

	# Spark SQLContext
	sqlContext = SQLContext( spark )

	# Read Bitcoin data from Cassandra
	df_bitcoin = spark.read.format( "org.apache.spark.sql.cassandra" ) \
		.options( table = config.cassandra["BITCOIN"], keyspace = config.cassandra["KEYSPACE"] ) \
		.load()


	df_bitcoin.registerTempTable( "bitcoin" )


	# Read Dark Web data from Cassandra
	df_marketplace = spark.read.format( "org.apache.spark.sql.cassandra" ) \
		.options( table = config.cassandra["MARKETPLACE"], keyspace = config.cassandra["KEYSPACE"] ) \
		.load() 

	df_marketplace.registerTempTable( "marketplace" )

	# Udf function to increment and return date num_days
	date_incr = ( udf( lambda date, num_days:  date + relativedelta( days=num_days ), TimestampType() ) )

	# Join
	result = df_bitcoin \
		.join( df_marketplace, ( df_bitcoin.recv_amount == df_marketplace.price ) & \
			( df_bitcoin.time > df_marketplace.ad_date ) & \
			( df_bitcoin.time <  date_incr( df_marketplace.ad_date, lit(10) ) ), 'inner' ) \
		.drop( df_bitcoin.recv_amount ) 


	# Write into Postgres
	write_postgres( result )







