# smack-exercise [![Build Status](https://travis-ci.org/kbelova/smack-exercise.svg?branch=master)](https://travis-ci.org/kbelova/smack-exercise)

This is example application of reading JSON tar.gz data with Spark, processing it, and publish to Cassandra.  Components of Smack being used: spark, cassandra. Everything is dockerized. Added Travis CI support.

## How to run with Docker

1. create directory `./data` and put your *.tar.gz inside
2. edit PATH inside docker-compose.yml point it to *.tar.gz, by default it's expecting: /data/yelp_dataset.tar.gz
3. sudo docker-compose up //starting  cassandra and app with spark