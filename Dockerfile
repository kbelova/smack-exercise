FROM openjdk:8

RUN apt-get -qq update
RUN wget www.scala-lang.org/files/archive/scala-2.11.11.deb \
    && dpkg -i scala-*
RUN wget https://d3kbcqa49mib13.cloudfront.net/spark-2.2.0-bin-hadoop2.7.tgz
RUN mkdir -p /opt/spark \
    && tar zxf spark-2.2.0-bin-hadoop2.7.tgz -C /opt/spark
RUN rm spark-2.2.0-bin-hadoop2.7.tgz \
    && rm scala-2.11.11.deb
RUN apt-get -y install maven

VOLUME ["/data/parse"]

ENV SPARK_HOME /opt/spark/spark-2.2.0-bin-hadoop2.7

COPY src ./src
COPY pom.xml .
RUN mvn clean package && mvn assembly:assembly

ARG PATH

ENTRYPOINT "${SPARK_HOME}/bin/spark-submit --conf spark.cassandra.connection.host=cassandra --class \"smackexercise.App\" ./target/smacktest-1.0-SNAPSHOT-jar-with-dependencies.jar /data/parse/${PATH}"