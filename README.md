# smack-exercise [![Build Status](https://travis-ci.org/kbelova/smack-exercise.svg?branch=master)](https://travis-ci.org/kbelova/smack-exercise)


## How to run with Docker
2. create directory `./data` and put your *.tar.gz inside
3. edit PATH inside docker-compose.yml point it to *.tar.gz, by default it's expecting: /data/yelp_dataset.tar.gz
4. sudo docker-compose up -d //starting  cassandra and app with spark