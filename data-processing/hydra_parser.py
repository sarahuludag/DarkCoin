

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




def is_float( str ):
	"""
	Is Float

	Check if given string is a valid float

	Parameters:
	str (string): string to be checked

	Returns:
	boolean: if it is a valid float

	"""

    try:
        float( str )
        return True

    except ValueError:
        return False



def parse_files( file_names ):
	"""
	Parse the given HTML files

	Parses Hydra HTML pages, extracts item details and stores it in items list

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
				if ( html_soup.find( "div", class_ = "span4 well bdgreen" ) ):
					price_content = html_soup.find( "div", class_ = "span4 well bdgreen" )

					# Check if they accept Bitcoin
					spans = price_content.find_all( "span" )
					for span in spans:

						# Check if bitcoin symbol is in span
						if ( "฿" in str( span ) ):

							# Parse price
							price = span.text[ 1: ]

							# Parse vendor
							vendor = ""
							if ( html_soup.find( "div", class_ = "span6 well bblue" ) ):
								vendor = html_soup.find( "div", class_ = "span6 well bblue" )
								vendor = vendor.h2.text.strip()

							
							if ( html_soup.find( "div", class_ = "span7" ) ):
								image = html_soup.find( "div", class_ = "span7" )
								image = image.find( "img" )

								# Parse product name
								product_name = image["alt"]

								# Parse image link
								image_id = image["src"]

								# Parse shipping information
								ship_from, ship_to = "", ""
								if ( html_soup.find( "ul", class_ = "activity" ) ):
									shipping_list = html_soup.find( "ul", class_ = "activity" )
									lis = shipping_list.find_all( "li" )

									# Join list of shipping places into a string
									for li in lis:

										# Parse ship_from
										if ( "from" in li.text.lower() ):
											ship_from = " ".join( li.text.split()[2:] )

										# Parse ship_to
										if ( "to" in li.text.lower() ):
											ship_to = " ".join( li.text.split()[2:] )


								if ( html_soup.find( "div", class_ = "widget span12 wblue" ) ):
									product_details = html_soup.find( "div", class_ = "widget span12 wblue" )

									# Parse product name
									product_name2 = html_soup.find( "div", class_ = "widget-head" ).text
									product_details = product_details.find( "div", class_ = "widget-content" )
									if ( product_name != product_name2 ):
										product_name = product_name2
									
									# Parse description
									desc = product_details.text

									# Check if product name and price exist and if price is valid
									if ( product_name and price and is_float( price ) ):
										
										# Add parsed item to the items list
										items.append( ( "hydra", product_name, float( price ), "", vendor, desc, datetime.strptime( date, '%Y-%m-%d' ), ship_to, ship_from, image_id ) )

										
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
	f = open( config.files["HYDRAFILES"], "r" )
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


