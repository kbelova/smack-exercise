# smack-exercise


## How to run
without Docker
1. git clone
2. mvn clean package
3. mvn assembly:assembly // to get fat jar
4. SPARK_HOME/bin/spark-submit --conf spark.cassandra.connection.host=cassandra -class App ./target/smacktest-1.0-SNAPSHOT-jar-with-dependencies.jar /path_to_input.tar.gz

with docker
