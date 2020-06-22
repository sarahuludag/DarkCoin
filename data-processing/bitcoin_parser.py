
# Import libraries
import json
import boto3
import itertools
from datetime import datetime

import cassandra

from pyspark.sql import *
from pyspark.sql.types import *
from pyspark.sql import SparkSession
from pyspark import SparkContext, SparkConf
from pyspark.sql.functions import explode, concat_ws, udf, concat, col, lit, when



def validateJSON( jsonData ):
	"""
	Check if file is in valid json file

	Try to load jsonData as a json file, if throws an exception return false.

	Parameters:
	jsonData (str): string that will be validated

	Returns:
	bool: if jsonData is in valid json format or not

	"""
    try:
        json.loads( jsonData )

    except ValueError as err:
        return False

    return True




def parse_files( file_names ):
	"""
	Parse the given Bitcoin blocks that are stored in json files

	Parses json files that store Bitcoin blocks, extracts transaction details and stores them in blocks list

	Parameters:
	file_names (list): list of file names to be parsed

	Returns:
	list: list of transactions that has been parsed

	"""

	# Use boto3 to access S3 bucket
	s3 = boto3.resource( 's3' )

	# Create a list to keep blocks parsed
	blocks=[]

	# For each file read from s3 and parse blocks
	for file_name in file_names:

		# Check if file name is valid
		if( file_name is not None and file_name is not "" ):

			# Read the file from S3 bucket and load into obj
			obj = s3.Object( config.s3["S3BUCKET"], file_name )
			if( obj is not None ):
				body = obj.get()['Body'].read().decode( 'utf-8' )

				# Validate the json file
				if( validateJSON( body ) ):

					# Load json into the data
					data = json.loads( body )

					# Parse the block number
					block_num = data["blocks"][0]["height"]

					# Parse the time
					time = datetime.fromtimestamp( ( float( data["blocks"][0]["time"] ) ) )

					# For each transaction stored in the Bitcoin block
					for tx in data["blocks"][0]["tx"]:

						# Parse transaction hash
						tx_hash = tx["hash"]

						# Find sending addresses and amounts in the tx and store them into tx_inputs list
						tx_inputs = []
						if ( "inputs" in tx.keys() ):
							# For each input in tx
							for inp in tx["inputs"]:

								# If the input has an address and value 
								if ( "prev_out" in inp.keys() and \
									"addr" in inp["prev_out"].keys() and \
									"value" in inp["prev_out"].keys() ):

									# Add the input into the tx_inputs list
									tx_inputs.append( [inp["prev_out"]["addr"], float( inp["prev_out"]["value"] ) / 100000000] ) 
												
						# Find receiving addresses and amounts in the tx and store them into tx_outs list
						tx_outs = []
						if ( "out" in tx.keys() ):
							# For each out in tx
							for out in tx["out"]:
								# If the out has an address and value 
								if ( "addr" in out.keys() and "value" in out.keys() ):
									# Add the out into the tx_outs list
									tx_outs.append( [out["addr"], float( out["value"] ) / 100000000] )

						# Perform Cartesian product on input and output addresses
						for inp in tx_inputs:
							for out in tx_outs:
								# Add each result to the blocks list
								blocks.append( [tx_hash, inp[0], inp[1], out[0], out[1], time, block_num] )

	# Return parsed blocks
	return blocks




def main():
	"""
	Main function

	Main function that distributes the files that will be parsed and writes the result into database

	"""

	# Read the list of Bitcoin block files in the S3
	file_list=[]
	f = open( config.files["BITCOINFILES"], "r" )
	for x in f:
		file_list.append( x.strip() )

	# Create Spark session
	appName = "BitcoinParser"
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
	
	n_parts = 1000	# Number of partitions

	# Distribute files among spark nodes and calculate result using Map-Reduce
	rdd_file = spark.sparkContext.parallelize( file_list, n_parts ) 
	rdd = rdd_file.mapPartitions( parse_files )


	# Create a schema for the dataframe	
	schema = StructType([
		StructField( 'tx_hash', StringType(), True ),
		StructField( 'send_addr', StringType(), True ),
		StructField( 'send_amount', FloatType(), True ),
		StructField( 'recv_addr', StringType(), True ),
		StructField( 'recv_amount', FloatType(), True ),
		StructField( 'time', DateType(), True ),
		StructField( 'block_num', StringType(), True )
	])


	# Create data frame
	df = spark.createDataFrame( rdd,schema )

	# Print example of data and statistics
	df.show()
	print( "-------------------Total rows:" + str( df.count() ) )


	# Write result to Cassandra
	df.write\
	  .format( "org.apache.spark.sql.cassandra" ) \
	  .mode( "append" ) \
	  .options( table = config.cassandra["BITCOIN"], keyspace = config.cassandra["KEYSPACE"] ) \
	  .save()









