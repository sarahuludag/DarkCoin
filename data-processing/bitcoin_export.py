
# Import libraries
from pyspark.sql import *
from pyspark.sql.types import *
from pyspark.sql import SparkSession
from pyspark import SparkContext, SparkConf
from pyspark.sql.functions import explode, concat_ws, udf, concat, col, lit, when

import cassandra

import config
from datetime import datetime
from dateutil.relativedelta import relativedelta


def main():
	"""
	Main function

	Main function that exports Bitcoin data on Cassandra to json files on S3

	"""

	# Create Spark session
	appName = "BitcoinExport"
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


	df_bitcoin = spark.read.format( "org.apache.spark.sql.cassandra" ) \
		.options( table = config.cassandra["BITCOIN"], keyspace = config.cassandra["KEYSPACE"] ) \
		.load()

	df_bitcoin.registerTempTable( "bitcoin" )

	# Run select query 
	result = spark.sql( "SELECT tx_hash, send_addr, recv_addr FROM bitcoin WHERE tx_hash IS NOT NULL AND send_addr IS NOT NULL AND recv_addr IS NOT NULL" )

	# Write result of the query into S3 as json file
	result.write.mode( 'append' ).json( 's3a://' + config.s3["S3BUCKET"] + '/bitcoin_transactions.json' )


if __name__ == "__main__":
    main()
