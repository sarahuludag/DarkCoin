# Create a list of silkroad2 websites on the S3 bucket
def list_file_names_silkroad2():
	from boto.s3.connection import S3Connection
	conn = S3Connection($ACCESSID,$ACCESSKEY)
	bucket = conn.get_bucket($S3BUCKET2)
	for key in bucket.list(prefix='silkroad2'): 
		if "silkroad2" in str(key.name) and "categories" in str(key.name):
			print key.name.encode('utf-8')

# Create a list of bitcoin block files on the S3 bucket
def list_file_names_bitcoin():
	from boto.s3.connection import S3Connection
	conn = S3Connection($ACCESSID,$ACCESSKEY)
	bucket = conn.get_bucket($S3BUCKET)
	for key in bucket.list():
		if "bitcoinblocks" in str(key.name) and "bitcoinblocks1516" not in str(key.name):
			print key.name.encode('utf-8')

list_file_names_silkroad2()
