

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

	Parses Diabolus HTML pages, extracts item details and stores it in items list

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
		if( file_name is not None and file_name is not "" ):

			# Read the file from S3 bucket and load into obj
			obj = s3.Object( config.s3["S3BUCKET2"], file_name )
			if( obj is not None ):	
				body = obj.get()['Body'].read()

				# Parse date from the file name
				x = file_name.split( "/" )
				date = x[1]

				# Load page into BeautifulSoup for parsing
				html_soup = BeautifulSoup( body, 'html.parser' )

				# Start parsing
				if( html_soup.find( "div", style = "min-width:275px" ) ):
					content = html_soup.find( "div", style = "min-width:275px" )

					# Parse product name
					product_name = None
					if( content.find( "h1" ) ):
						product_name = content.find( "h1" ).text

					if( content.find( "h3" ) ):
						product_name = content.find( "h3" ).text

					if( content.find( "h2" ) ):
						product_name = content.find( "h2" ).text

					if( content.find( "h4" ) ):
						product_name = content.find( "h4" ).text
					

					# Parse image link if it exists
					image_id = ""
					if( content.find( "a", target = "_blank" ) ):
						image_id = content.find( "a", target = "_blank" ).get( "href" )
						

					if( content.find_all( "span", class_ = "form-control" ) ):
						spans = content.find_all( "span", class_ = "form-control" )
						
						# Check if Bitcoin is accepted
						if( spans[0].find( "img", src = "btc.png" ) ):
							if ( spans[1].text.split( "/" ) ):

								# Parse price
								prices = spans[1].text.split( "/" )
								for price_item in prices:
									if "BTC" in price_item:
										price = price_item.strip().split()[0]
										

							# Check if product name and price exists
							if( product_name and price ):
								
								# Parse vendor
								vendor = ""
								if ( "Vendor" in spans[2].text ):
									vendor = spans[2].find( "a" ).text
									
								# Parse shipping information if exists
								# Parse ship_from
								ship_from = ""
								if ( "from" in spans[3].text.lower() ):
									ship_from = " ".join( ( spans[3].text ).split()[2] )
									
								# Parse ship_to
								ship_to = ""
								if ( "to" in spans[4].text.lower() ):
									ship_to = " ".join( ( spans[4].text ).split()[2] )
									

								# Parse category if exists
								category = ""
								if ( "category" in spans[7].text.lower() ):
									category = ( spans[7].text ).split()[1]
									

								# Parse description if exists
								desc = ""
								if( content.find( "div", id = "cats" ) ):
									desc = content.find( "div", id = "cats" ).text

								# Add parsed item to the items list
								items.append( ( "diabolus", product_name, float( price ), category, vendor, desc, datetime.strptime( date, '%Y-%m-%d' ), ship_to, ship_from, image_id ) )

										
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
	f = open( config.files["DIABOLUSFILES"], "r" )
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


