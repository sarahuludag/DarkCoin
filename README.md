# [DarkCoin](https://www.dark-co.in/)

Bitcoin transaction tracker for Dark Web marketplaces

You can check out the presentation [here](https://bit.ly/DarkCoinPresent).


## Introduction

The problem is the usage of Bitcoin for illegal transactions on Dark Web marketplaces because it is hard to trace back.

The goal of the project is to find and mark Bitcoin public keys that are possibly related to a Dark Web transactions and other public keys that are related to these “dirty” keys.

## Architecture

![Pipeline](images/pipeline.png)

## Dataset

HTML files of Dark Web marketplaces scraped on 2014 and 2015, stored in a S3 bucket.

Data of Bitcoin blocks acquired from blockchain.com API by running multithreaded bash scripts on EC2 instance and stored in a S3 bucket.

## Engineering challenges

* Getting data of each Bitcoin block with HTTP requests from the API is a very slow process. Used a multithreading approach to speed up process.

* Creating schema and parser for each marketplace since they have different format and data.

* Creating a unified data model for the Dark Web markets that would work with all parsers.

* Using Map-Reduce to boost parsing operations.

* Partitioning data on Cassandra according data according to date column so they will be close to each other in database. It speeded up writing/reading process while reading from Spark which used date column for filtering. 

* Preprocessing and Denormalization of the resulting database to achieve fast querying and filtering.

