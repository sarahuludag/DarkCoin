

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

	Parses Agora HTML pages, extracts item details and stores it in items list

	Parameters:
	file_names (list): list of file names to be parsed

	Returns:
	list: list of items that has been parsed

	"""

	sys.setrecursionlimit(10000)

	# Use boto3 to access S3 bucket
	s3 = boto3.resource('s3')

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
				categories = [] # List to store categories
				# Parse categories if exists
				if( html_soup.find_all( "div", class_ = "topnav-element" ) ):
					cats = html_soup.find_all( "div", class_ = "topnav-element" )

					# For each category parsed, add it to the categories list
					for category in cats:
						if( category.find( "a" ) ):
							categories.append( category.find( "a" ).text )
					
					# Find products list item
					if( html_soup.find_all( "tr", class_ = "products-list-item" ) ):
						products = html_soup.find_all( "tr", class_ = "products-list-item" )

						# For each row in products
						for row in products:

							# Parse image link if it exists
							image_id = ""
							if( row.find( "td", style = "text-align: center;" ) ):
								if( row.find( "td", style = "text-align: center;" ).find( "img" ) ):
									image_id = str( row.find( "td", style = "text-align: center;" ).find( "img" )["src"] )
									

							# Find product name and item description
							if( row.find( "td", class_ = "column-name" ) ):
								if( row.find( "td", class_ = "column-name" ).a ):

									# Parse product name
									product_name = str( row.find( "td", class_ = "column-name" ).a.text ).strip()

									# Parse description if it exists
									desc = ""
									if( row.find( "td", class_ = "column-name" ).span ):
										desc = row.find( "td", class_ = "column-name" ).span.text.strip()
										

									# Parse price
									if( row.find_next( "td" ).find_next( "td" ).find_next( "td" ) ):
										price_text = row.find_next( "td" ).find_next( "td" ).find_next( "td" ).text

										# Check if price is Bitcoin
										if " BTC" in price_text:
											price = price_text.split(" ")[0]
									
											# Parse shipping information
											ship_to, ship_from = "", ""
											if( row.find( "td", style = "white-space: nowrap;" ) ):
												shipping = row.find( "td", style = "white-space: nowrap;" )

												# Parse ship_from
												if ( shipping.find( "img", class_ = "flag-img" ) and \
													shipping.find( "i", class_  = "fa fa-truck" ) and \
													shipping.find( "i", class_ = "fa fa-truck" ).next_sibling ):							
													ship_from = shipping.find( "i", class_ = "fa fa-truck" ).next_sibling.next_sibling

												# Parse ship_to
												if ( shipping.find( "i", class_ = "fa fa-home" ) ):
													ship_to = str( shipping.find( "i", class_ = "fa fa-home" ).next_sibling ).strip().split( " " )[-1]
													

											# Parse vendor
											vendor = ""
											if ( row.find( "a", class_ = "gen-user-link" ) ):
												vendor = str( row.find( "a", class_ = "gen-user-link" ).next_sibling )
											
											# Join categories into a string	
											categories_str = str( "/".join( categories ) )

											# Add parsed item to the items list
											items.append( ( "agora", product_name, float( price ), categories_str, vendor, desc, datetime.strptime( date, '%Y-%m-%d' ), ship_to, ship_from, image_id ) )

										
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



