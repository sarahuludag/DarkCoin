
# Create a list of websites on the S3 bucket
def list_file_names( website_name, folder_name ):
	from boto.s3.connection import S3Connection
	conn = S3Connection( $ACCESSID, $ACCESSKEY )
	bucket = conn.get_bucket( $S3BUCKET )
	for key in bucket.list( prefix = website_name ): 
		if website_name in str( key.name.encode( 'utf-8' ) ) and folder_name in str( key.name.encode( 'utf-8' ) ):
			print key.name.encode( 'utf-8' )

# Create a list of bitcoin block files on the S3 bucket
def list_file_names_bitcoin():
	from boto.s3.connection import S3Connection
	conn = S3Connection( $ACCESSID, $ACCESSKEY )
	bucket = conn.get_bucket( $S3BUCKET )
	for key in bucket.list():
		if "bitcoinblocks" in str( key.name ):
			print key.name.encode( 'utf-8' )

list_file_names( "evolution", "listing" )
list_file_names( "cryptomarket", "cm.php" )
list_file_names( "diabolus", "item.php" )
list_file_names( "cloudnine", "group_id" )
list_file_names( "hydra", "sale" )
list_file_names( "agora", "cat" )
list_file_names( "silkroad2", "categories" )
list_file_names_bitcoin()