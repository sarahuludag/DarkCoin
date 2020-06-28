

# Import libraries
from bs4 import BeautifulSoup	# BeautifulSoup for parsing HTML

import sys
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

	Parses Pandora HTML pages, extracts item details and stores it in items list

	Parameters:
	file_names (list): list of file names to be parsed

	Returns:
	list: list of items that has been parsed

	"""

	sys.setrecursionlimit( 10000 )

	# Use boto3 to access S3 bucket
	s3 = boto3.resource( 's3' )

	# Create a list to keep items parsed
	items = []

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
				if ( html_soup.find( "div", id = "content" ) ):

					content = html_soup.find( "div", id = "content" )
					if ( html_soup.find( "table", class_ = "width70" ) ):

						# Crate variables
						product_name = ""
						image_id = ""
						vendor = ""
						price = ""
						ship_from = ""
						ship_to = ""

						# Find table
						table = html_soup.find( "table", class_ = "width100" )
						for row in table.find_all( "tr" ):

							# Parse product name
							if ( row.find( "th", colspan = "2" ) ):
								product_name = row.find( "th", colspan = "2" ).text

							# Parse image link
							elif ( row.find( "td", rowspan = "6" ) ):
								image_id = row.find( "td", rowspan = "6" ).find( "img" )["src"]

								# Parse vendor
								if ( row.td.find_next( "td" ).text == "Seller:" ):
									vendor = row.td.find_next( "td" ).find_next( "td" )
									vendor = vendor.find( "a" ).text

							elif ( row.td ):

								# Parse price
								if ( row.td.text == "Price:" ):
									if ( row.td.find_next( "td" ).text.find( "฿" ) ):
										price = row.td.find_next( "td" ).text.split( "฿" )[1].split( " " )[0]

								# Parse shipping information
								elif ( row.td.text == "Shipping from:" ):
									shipping_from = row.td.find_next( "td" ).text

								elif ( row.td.text == "Shipping to:" ):
									shipping_to = row.td.find_next( "td" ).text

						# Get description if exists
						desc = ""		
						if ( html_soup.find( "pre" ) ):
							desc = html_soup.find( "pre" ).text

						# Check if product name and price exist
						if ( product_name and price ):

							# Add parsed item to the items list
							items.append( ( "pandora", product_name, float( price ), "", vendor, desc, datetime.strptime( date, '%Y-%m-%d' ), ship_to, ship_from, image_id ) )								

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
	f = open( config.files["PANDORAFILES"], "r" )
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

