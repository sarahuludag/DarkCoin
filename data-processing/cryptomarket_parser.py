

# Import libraries
from bs4 import BeautifulSoup	# BeautifulSoup for parsing HTML

import boto3
import itertools
from datetime import datetime
import config

import cassandra

from pyspark.sql import *
from pyspark.sql.types import *
from pyspark.sql import SparkSession
from pyspark import SparkContext, SparkConf
from pyspark.sql.functions import explode, concat_ws, udf, concat, col, lit, when




def parse_files( file_names ):
	"""
	Parse the given HTML files

	Parses Cryptomarket HTML pages, extracts item details and stores it in items list

	Parameters:
	file_names (list): list of file names to be parsed

	Returns:
	list: list of items that has been parsed

	"""

	# Use boto3 to access S3 bucket
	s3 = boto3.resource( 's3' )

	# Create a list to keep items parsed
	items=[]

	# For each file read from s3 and parse items
	for file_name in file_names:

		# Check if file name is valid
		if ( file_name is not None and file_name is not "" ):

			# Read the file from S3 bucket and load into obj
			obj = s3.Object( config.s3["S3BUCKET2"], file_name )
			if ( obj is not None ):	
				body = obj.get()['Body'].read()

				# Parse date from the file name
				x = file_name.split( "/" )
				date = x[1]

				# Load page into BeautifulSoup for parsing
				html_soup = BeautifulSoup( body, 'html.parser' )

				# Start parsing
				if ( html_soup.find_all( "div", id = "img" ) ):
					# Find contents
					contents = html_soup.find_all( "div", id = "img" )

					# For each content in contents
					i = 0
					for content in contents:
						i +=1

						# Parse image link
						if ( content.find( "img", style = "width:80px; height:80px" ) and content.find( "div", id = "img" ) ):
							images = content.find_all( "img", style = "width:80px; height:80px" )
							image_id = images[0]["src"]


						if ( content.find( "div", style = "min-width:200px" ) ):

							# Parse product name
							product_name = content.find( "div", style = "min-width:200px" ).find( "a" ).text

							# Parse price
							price_raw = content.find( "b", style = "color:#fff665" )
							price = price_raw.text.split( "/" )[1].split()[0]
							
							# Check if product name and price is not null
							if ( product_name and price ):

								# Parse vendor
								vendor = price_raw.find_next( "a" ).text

								# Parse category
								category = price_raw.find_next( "a" ).find_next( "a" ).text

								# Parse shipping information
								ship_from = price_raw.find_next( "b", id = "img" ).text
								ship_to = price_raw.find_next( "b", id = "img" ).find_next( "b", id = "img" ).text

								# Add parsed item to the items list
								items.append( ( "cryptomarket", product_name, float( price ), category, vendor, "", datetime.strptime( date, '%Y-%m-%d' ), ship_to, ship_from, image_id ) )

										
	# Return parsed items
	return items




def main():
	"""
	Main function

	Main function that distributes the files that will be parsed and writes the result into database

	"""

	# Get list of names of the files that will be parsed
	file_list=[]

	# Open the file and read file names
	f = open( config.files["CRYPTOMARKETFILES"], "r" )
	for x in f:
		file_list.append( x.strip() )

	# Create Spark session
	appName = "BeautifulSoup"
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

	# Number of partitions
	n_parts = 100

	# Distribute files among worker nodes using Map-Reduce and put results into rdd
	rdd_file = spark.sparkContext.parallelize( file_list, n_parts ) 
	rdd = rdd_file.mapPartitions( parse_files )


	# Create a schema for the dataframe	
	schema = StructType([
		StructField( 'marketplace', StringType(), True ),
		StructField( 'product_name', StringType(), True ),
		StructField( 'price', FloatType(), True ),
		StructField( 'category', StringType(), True ),
		StructField( 'vendor', StringType(), True ),
		StructField( 'description', StringType(), True ),
		StructField( 'ad_date', DateType(), True ),
		StructField( 'ship_to', StringType(), True ),
		StructField( 'ship_from', StringType(), True ),
		StructField( 'image_url', StringType(), True )
	])


	# Create data frame from rdd using the schema above
	df = spark.createDataFrame( rdd,schema )

	# Print example of data and statistics
	df.show()
	print( "-------------------Total rows:" + str( df.count() ) )


	# Write result to Cassandra
	df.write\
	  .format( "org.apache.spark.sql.cassandra" ) \
	  .mode( "append" ) \
	  .options( table = config.cassandra["MARKETPLACE"], keyspace = config.cassandra["KEYSPACE"] ) \
	  .save()





if __name__ == "__main__":
    main()





