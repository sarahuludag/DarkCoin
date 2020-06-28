

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

	Parses Evolution HTML pages, extracts item details and stores it in items list

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
				categories = [] # List to store categories
				# Parse categories if exists
				if ( html_soup.find( "ol", class_ = "breadcrumb" ) ):
					breadcrumb = html_soup.find( "ol", class_ = "breadcrumb" )
					cats = breadcrumb.find_all( "li" )

					# For each category parsed, add it to the categories list
					for category in cats:
						categories.append( category.text )
					

				# Parse image link if it exists
				image_id = ""
				if ( html_soup.find( "div", class_ = "col-md-5" ) \
					and html_soup.find( "div", class_ = "col-md-5" ).find( "a", class_ = "thumbnail" ) ):

					image_id2 = html_soup.find( "div", class_ = "col-md-5" ).find( "a", class_ = "thumbnail" ).get( "href" )
					image_id = "/".join( image_id2.split( "/" )[ 3: ] )


				if ( html_soup.find( "div", class_ = "col-md-7" ) ):
					info_column = html_soup.find( "div", class_ = "col-md-7" )

					# Parse product name 
					product_name = ""
					if ( info_column.h3 ):
						product_name = info_column.h3.text
		
					elif ( info_column.h1 ):
						product_name = info_column.h1.text

					elif ( info_column.h2 ):
						product_name = info_column.h2.text

					elif ( info_column.h4 ):
						product_name = info_column.h4.text	

					# Check if product name exists
					if ( product_name ):

						# Parse vendor
						vendor = ""
						if ( info_column.find( "div", class_ = "seller-info text-muted" ) \
							and info_column.find( "div", class_ = "seller-info text-muted" ).find( "a" ) ):

							vendor2 = info_column.find( "div", class_ = "seller-info text-muted" )
							vendor = vendor2.find( "a" ).text


						# Parse price
						price = ""
						if ( info_column.find( "h4", class_ = "text-info" ) ):
							price2 = info_column.find( "h4", class_ = "text-info" ).text
							price = price2.split( " " )[1]

						elif ( info_column.find( "h3", class_ = "text-info" ) ):
							price2 = info_column.find( "h3", class_ = "text-info" ).text
							price = price2.split( " " )[1]
						

						# Check if price exists
						if ( price ):  

							# Parse description if exists
							desc = ""
							if ( html_soup.find( "div", class_ = "product-summary" ) ):
								desc = html_soup.find( "div", class_ = "product-summary" ).p.text


							# Parse shipping information
							# Parse ship_to
							ship_to = ""
							if ( html_soup.find_all( "div", class_ = "col-md-9" ) \
								and len( html_soup.find_all( "div", class_ = "col-md-9" ) ) > 1 ):	
 
								ship_to2 = html_soup.find_all( "div", class_ = "col-md-9" )[1]
								if ( ship_to2.find_all( "p" ) and len( ship_to2.find_all( "p" ) ) > 1 ):
									ship_to = str( ship_to2.find_all( "p" )[1].text )

							# Parse ship_from
							ship_from = ""
							if ( html_soup.find( "div", class_ = "widget" ) ):
								widgets = html_soup.find_all( "div", class_ = "widget" )
								for widget in widgets:
									if widget.h3 and widget.h3.text == "Ships From":
										ship_from = widget.p.text

							# Join categories into a string
							categories_str = str( "/".join( categories ) )

							# Add parsed item to the items list
							items.append( ( "evolution", product_name, float( price ), categories_str, vendor, desc, datetime.strptime( date, '%Y-%m-%d' ), ship_to, ship_from, image_id ) )
										
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
	f = open( config.files["AGORAFILES"], "r" )
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

