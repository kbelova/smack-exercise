package smackexercise

import com.datastax.spark.connector.cql.CassandraConnector
import com.typesafe.config.ConfigFactory
import org.apache.spark.SparkConf
import smackexercise.model.SourceEnum

object CassandraHelper {
  private val appConfig = ConfigFactory.load()
  private val strategy = appConfig.getString("cassandra.strategy")
  private val replicaFactor = appConfig.getString("cassandra.replication_factor")
  private var updateSchema = appConfig.getBoolean("cassandra.update_schema")

  @transient  private var cc: CassandraConnector = _

  val keyspace: String = appConfig.getString("cassandra.keyspace")

  def getInstance(sparkConf: SparkConf): CassandraConnector = {
    if (cc == null) {
      println("NEW CASSANDRA CONNECTOR")
      cc = CassandraConnector(sparkConf)
    }
    cc
  }
  def checkSchemaUpdate(sparkConf: SparkConf) = {
    if (updateSchema) {
      updateSchema = createSchema(keyspace, strategy, replicaFactor, sparkConf)
    }
  }

  private def createSchema(keyspace: String, strategy: String, replica_factor: String, sparkConf: SparkConf): Boolean = {


      getInstance(sparkConf).withSessionDo { session => {
        session.execute(s"CREATE KEYSPACE IF NOT EXISTS $keyspace WITH replication = {"
          + s" 'class': '${strategy}', "
          + s" 'replication_factor': '${replica_factor}' "
          + "};");
        session.execute(s"use ${keyspace};")

        SourceEnum.values.foreach(sourceFiles => {
          session.execute(s"DROP TABLE IF EXISTS ${sourceFiles.name}")
          //create table with CQL from app config
          session.execute(appConfig.getString(s"cassandra.tables.${sourceFiles.name}.schema"))
        })
      }
      }
    //to update schema only once during programm execution
      false
    }
}
