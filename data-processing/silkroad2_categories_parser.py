from bs4 import BeautifulSoup
from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, concat_ws, udf, concat, col, lit, when
from pyspark.sql.types import *
import boto3
import itertools
from datetime import datetime
from pyspark.sql import *
import cassandra


def parse_files(file_names):
	s3 = boto3.resource('s3')
	items=[]
	for file_name in file_names:
		if(file_name is not None and file_name is not ""):
			obj = s3.Object($S3BUCKET2, file_name)
			if(obj is not None ):	
				body = obj.get()['Body'].read()

				x = file_name.split("/")
				date = x[1]
				category = x[3]

				html_soup = BeautifulSoup(body, 'html.parser')
				item_container = html_soup.find_all("div", class_ = "item")
				for item in item_container: 
					if(item.find("div", class_="item_title")):
						title = item.find("div", class_="item_title")
						link = title.a
						product_name = title.a.text.strip()
						href = link.get("href")
						
						if(item.find("div", class_="item_details")):
							details = item.find("div", class_="item_details")
							if(details.a):
								vendor = details.a.text.strip()
								ship_from = details.br.next_sibling.strip()[12:]
								ship_to = details.find_all('br')[-1].next_sibling.strip()[10:]

								if(item.find("div", class_="price_big")):
									price = item.find("div", class_="price_big").text.strip()[1:]
									items.append((product_name, float(price), category, vendor, datetime.strptime(date, '%Y-%m-%d'), ship_to, ship_from, href))

	return items


file_list=[]
f = open($SILKROADFILES, "r")
for x in f:
	file_list.append(x.strip())

appName = "BeautifulSoup"
master = $MASTERIP

# Create Spark session
spark = SparkSession.builder \
    .appName(appName) \
    .config("spark.cassandra.connection.host", $CASSANDRAHOSTS) \
  	.config('spark.cassandra.connection.port', $CASSANDRAPORT) \
  	.config('spark.cassandra.output.consistency.level','ONE') \
  	.config('spark.kryoserializer.buffer.max','2047m') \
  	.config('spark.driver.port',$DRIVERPORT) \
    .getOrCreate()

# Distribute files among spark nodes
n_parts = 10
rdd_file = spark.sparkContext.parallelize(file_list, n_parts)
rdd = rdd_file.mapPartitions(parse_files)


# Create a schema for the dataframe	
schema = StructType([
	StructField('product_name', StringType(), True),
	StructField('price', FloatType(), True),
	StructField('category', StringType(), True),
	StructField('vendor', StringType(), True),
	StructField('ad_date', DateType(), True),
	StructField('ship_to', StringType(), True),
	StructField('ship_from', StringType(), True),
	StructField('href', StringType(), True)
])


# Create data frame
df = spark.createDataFrame(rdd,schema)
print(df.schema)
df.show()
print("-------------------Total rows:" + str(df.count()))


# Write to Cassandra
df.write\
  .format("org.apache.spark.sql.cassandra") \
  .mode("append") \
  .options(table = $CASSANDRASILKROAD, keyspace = $CASSANDRAKEYSPACE) \
  .save()

# Read from Cassandra and show
df2 = spark.read.format("org.apache.spark.sql.cassandra") \
	.options(table=$CASSANDRASILKROAD, keyspace=$CASSANDRAKEYSPACE) \
	.load()

df2.show()








