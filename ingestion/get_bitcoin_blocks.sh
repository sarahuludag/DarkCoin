#!/bin/bash

# Set first and last Bitcoin block that will be ingested from API
for ((i = $BLOCKSTART; i <= $BLOCKEND; i += 50))
do  
	# Divide into chuncks to parallelize
	for (( j = i; j < i+50; j++ ))
	do
		echo "Sending block $j" 

		BLOCKHEIGHT = $j   
		echo $BLOCKHEIGHT 

		FILENAME="block"${j}".json"   
		curl -s "https://blockchain.info/block-height/$j?format=json&api_code=$APICODE" | aws s3 cp - s3://$S3BUCKET/${FILENAME}&
	done
	wait
done

