
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

	Parses Silkroad2 HTML pages, extracts item details and stores it in items list

	Parameters:
	file_names (list): list of file names to be parsed

	Returns:
	list: list of items that has been parsed

	"""

	# Use boto3 to access S3 bucket
	s3 = boto3.resource( 's3' )

	# Create a list to keep items parsed
	items = []

	# For each file read from s3 and parse items
	for file_name in file_names:

		# Check if file name is valid
		if( file_name is not None and file_name is not "" ):

			# Read the file from S3 bucket and load into obj
			obj = s3.Object( config.s3["S3BUCKET2"], file_name )
			if( obj is not None ):	
				body = obj.get()['Body'].read()

				# Parse date and category name from the file name
				x = file_name.split( "/" )
				date = x[1]	# Date of the ad
				category = x[3]	# Category of the item

				# Load page into BeautifulSoup for parsing
				html_soup = BeautifulSoup( body, 'html.parser' )

				# Start parsing
				item_container = html_soup.find_all( "div", class_ = "item" )
				for item in item_container: 

					# Find title
					if( item.find( "div", class_ = "item_title" ) ):
						title = item.find( "div", class_ = "item_title" )

						# Parse product name and href form the title
						if( title.a ):
							link = title.a
							product_name = title.a.text.strip()	# Name of the item
							href = link.get( "href" )	# Link of the image of the item
							
							# Find item details
							if( item.find( "div", class_ = "item_details" ) ):
								details = item.find( "div", class_ = "item_details" )

								# Parse vendor
								if( details.a ):
									vendor = details.a.text.strip()	# Vendor of the item

									# Parse shipping details, Give null value if not exists
									if( details.br ):
										ship_from = details.br.next_sibling.strip()[12:]
										ship_to = details.find_all('br')[-1].next_sibling.strip()[10:]
									else:
										ship_from = " "
										ship_to = " "

									# Parse price of the item
									if( item.find( "div", class_ = "price_big" ) ):
										price = item.find( "div", class_ = "price_big" ).text.strip()[1:]	# Price of the item

										# Add parsed item to the items list
										items.append( ( "silkroad2", product_name, float( price ), category, vendor, "", datetime.strptime( date, '%Y-%m-%d' ), ship_to, ship_from, href ) )

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
	f = open( config.files["SILKROAD2FILES"], "r" )
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



