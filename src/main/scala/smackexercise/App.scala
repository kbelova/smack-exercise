package smackexercise

import com.typesafe.config._
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming._
import smackexercise.model.SourceEnum._
import smackexercise.model._
import uncompress.{ArchiveHelper, DataSender}


/**
  * @author ekaterina.belova
  */
object App {
  val appConfig = ConfigFactory.load()
  var path = ""
  /**thread with message queue, sending lines from archive to our localhost:port
  on which spark stream is listening*/
  val dataSocketSender = new Thread(DataSender)

  /**separate thread where we smackexercise.uncompress tar.gz and pass lines from it into message queue
  since smackexercise.uncompress can't be done parallel I considered to take it into a separate flow
  otherwise we have to wait until smackexercise.uncompress is done.*/
  val unCompresser = new Thread(new Runnable {
    override def run(): Unit = {
      new ArchiveHelper(path).processArchive
    }
  })

  object ContextKeeper {
    val sparkConfig: SparkConf = new SparkConf()
                                      .setMaster(appConfig.getString("spark.master"))
                                      .setAppName(appConfig.getString("spark.appname"))
    val ssc: StreamingContext = new StreamingContext(sparkConfig, Seconds(appConfig.getInt("spark.streaming.batch_duration")))
    val cassandra = CassandraHelper.getInstance(sparkConfig)
  }

  def main(args : Array[String]) {
    path = args(0)
    if (path == null || path.isEmpty) throw new RuntimeException ("Path to a tar.gz is null or empty")

    import ContextKeeper._
    //as a first step we check if there is a need to update schema in cassandra
    //can be false in case if we don't want to lost data keeped in tables from previous runs

    //update cassandra schema if needed
    CassandraHelper.checkSchemaUpdate(sparkConfig)

    // create stream from host:port on which our message queue sends lines from .tar.gz
    val streaming = ssc.socketTextStream( appConfig.getString("spark.streaming.server"),
                                          appConfig.getInt("spark.streaming.port"))
    streaming.foreachRDD((line: RDD[String]) => {
      val rdd = line.map(l => Record(l))
      val spark =  SparkSessionSingleton.getInstance(ssc.sparkContext.getConf)
      import com.datastax.spark.connector._

      rdd.filter(r => r.isMatch(PHOTOS.name))
            .map[Photos](p => Photos.apply(p))
            .saveToCassandra(CassandraHelper.keyspace, PHOTOS.name)

        rdd.filter(r => r.isMatch(USER.name))
          .map[User](u => User.apply(u))
          .saveToCassandra(CassandraHelper.keyspace, USER.name)

        rdd.filter(r => r.isMatch(REVIEW.name))
          .map[Review](r => Review.apply(r))
          .saveToCassandra(CassandraHelper.keyspace, REVIEW.name)

        rdd.filter(r => r.isMatch(TIP.name))
          .map[Tip](r => Tip.apply(r))
          .saveToCassandra(CassandraHelper.keyspace, TIP.name)

        rdd.filter(r => {r.isMatch(CHECKIN.name)})
          .flatMap[Checkin](r => Checkin.apply(r))
          .saveToCassandra(CassandraHelper.keyspace, CHECKIN.name)

        rdd.filter(r => {r.isMatch( BUSINESS.name)})
          .map[Business](b => Business.apply(b))
          .saveToCassandra(CassandraHelper.keyspace, BUSINESS.name)
      })

    dataSocketSender.start()
    ssc.start()
    unCompresser.start()
    /**
      * Blocks main tread until threads've been joined, are not finished.
      * Spark has it out of the box.
      * */
    dataSocketSender.join()
    unCompresser.join()
    println("WE ARE DONE")
    ssc.stop(true, true)
  }

  /** Lazily instantiated singleton instance of SparkSession */
  object SparkSessionSingleton {

    @transient  private var instance: SparkSession = _

    def getInstance(sparkConf: SparkConf): SparkSession = {
      if (instance == null) {
        instance = SparkSession
          .builder
          .config(sparkConf)
          .getOrCreate()
      }
      instance
    }
  }

}
