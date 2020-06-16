from bs4 import BeautifulSoup
from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, concat_ws, udf, concat, col, lit, when
from pyspark.sql.types import *
import boto3
import itertools
import json
from datetime import datetime
from pyspark.sql import *
import cassandra


def validateJSON(jsonData):
	"""
	Check if file is in valid json file

	Try to load jsonData as a json file, if throws an exception return false.

	Parameters:
	jsonData (str): string that will be validated

	Returns:
	bool: if jsonData is in valid json format or not

	"""
    try:
        json.loads(jsonData)
    except ValueError as err:
        return False
    return True


def parse_files(file_names):
	s3 = boto3.resource('s3')
	blocks=[]
	for file_name in file_names:
		if(file_name is not None and file_name is not ""):
			obj = s3.Object($S3BUCKET, file_name)
			if(obj is not None ):
				body = obj.get()['Body'].read().decode('utf-8')
				if(validateJSON(body)):
					data = json.loads(body)

					# Get the block number
					block_num = data["blocks"][0]["height"]
					time = datetime.fromtimestamp((float(data["blocks"][0]["time"])))
					for tx in data["blocks"][0]["tx"]:
						# Get transaction hash and time
						tx_hash = tx["hash"]
						# Find sending addresses and amounts
						tx_inputs = []
						if ("inputs" in tx.keys()):
							for inp in tx["inputs"]:
								if ("prev_out" in inp.keys() and "addr" in inp["prev_out"].keys() and "value" in inp["prev_out"].keys()):
									tx_inputs.append([inp["prev_out"]["addr"], float(inp["prev_out"]["value"])/100000000]) 
						# Find receiving addresses and amounts
						tx_outs = []
						if ("out" in tx.keys()):
							for out in tx["out"]:
								if ("addr" in out.keys() and "value" in out.keys()):
									tx_outs.append([out["addr"], float(out["value"])/100000000])

						# Merge input and output addresses
						for inp in tx_inputs:
							for out in tx_outs:
								blocks.append([tx_hash, inp[0], inp[1], out[0], out[1], time, block_num])

	return blocks

# Read the list of Bitcoin blocj=k files in the S3
file_list=[]
f = open($BITCOINFILES, "r")
for x in f:
	file_list.append(x.strip())

appName = "BitcoinParser"
master = $MASTERIP

# Create Spark session
spark = SparkSession.builder \
    .appName(appName) \
    .config("spark.cassandra.connection.host", $CASSANDRAHOSTS) \
  	.config('spark.cassandra.connection.port', $CASSANDRAPORT) \
  	.config('spark.cassandra.output.consistency.level','ONE') \
  	.config('spark.kryoserializer.buffer.max','2047m') \
  	.config('spark.driver.port', $DRIVERPORT) \
    .getOrCreate()

# Distribute files among spark nodes
n_parts = 1000
rdd_file = spark.sparkContext.parallelize(file_list, n_parts) 
rdd = rdd_file.mapPartitions(parse_files)


# Create a schema for the dataframe	
schema = StructType([
	StructField('tx_hash', StringType(), True),
	StructField('send_addr', StringType(), True),
	StructField('send_amount', FloatType(), True),
	StructField('recv_addr', StringType(), True),
	StructField('recv_amount', FloatType(), True),
	StructField('time', DateType(), True),
	StructField('block_num', StringType(), True)
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
  .options(table = $CASSANDRABITCOIN, keyspace = $CASSANDRAKEYSPACE) \
  .save()

# Read from Cassandra and show
df2 = spark.read.format("org.apache.spark.sql.cassandra") \
	.options(table=$CASSANDRABITCOIN, keyspace=$CASSANDRAKEYSPACE) \
	.load()

df2.show()







