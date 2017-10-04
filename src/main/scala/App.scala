import com.typesafe.config._
import model._
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming._
import org.apache.spark.sql.SparkSession
import com.datastax.spark.connector._
import com.datastax.spark.connector.cql.CassandraConnector
import com.datastax.spark.connector.writer.WriteConf
import org.apache.spark.sql.cassandra._

/**
  * @author ${user.name}
  */
object App {
  val appConfig = ConfigFactory.load()
  var path = ""
  val dataSocketSender = new Thread(DataSender)
  val unCompresser = new Thread(new Runnable {
    override def run(): Unit = {
      new GzipedArchive(path).processArchive
    }
  })
  object ContextKeeper {
    val sparkConfig = new SparkConf().setMaster(appConfig.getString("spark.master")).setAppName(appConfig.getString("spark.appname"))
    val ssc  = new StreamingContext(sparkConfig, Seconds(appConfig.getInt("spark.streaming.batch_duration")))
    val cc =  CassandraConnector(sparkConfig)
  }


  def createCassandraSchema(spark: SparkSession) : Boolean = {
    import spark.implicits._
    import com.datastax.spark.connector._
    ContextKeeper.cc.withSessionDo { session => {
      session.execute("CREATE KEYSPACE IF NOT EXISTS test WITH replication = {"
        + " 'class': 'SimpleStrategy', "
        + " 'replication_factor': '1' "
        + "};" );
      Array("business", "photos", "review", "tip", "user", "checkin").foreach(f => {
        session.execute(s"DROP TABLE IF EXISTS test.$f")
      })
    }
    }
    spark.emptyDataset[Photos].toDF.createCassandraTable("test", "photos")
    spark.emptyDataset[User].toDF.createCassandraTable("test", "user")
    spark.emptyDataset[Business].toDF.createCassandraTable("test", "business")
    spark.emptyDataset[Checkin].toDF.createCassandraTable("test", "checkin")
    spark.emptyDataset[Review].toDF.createCassandraTable("test", "review")
    spark.emptyDataset[Tip].toDF.createCassandraTable("test", "tip")
    false
  }

  def main(args : Array[String]) {
    var rewriteSchema = true
    import ContextKeeper._
    path = args(0)
    if (path == null || path.isEmpty) throw new RuntimeException ("Path to a tar.gz is null or empty")

    val streaming = ssc.socketTextStream(appConfig.getString("spark.streaming.server"), appConfig.getInt("spark.streaming.port"))
      streaming.foreachRDD((tuple: RDD[String]) => {
        val spark =  SparkSessionSingleton.getInstance(tuple.sparkContext.getConf)
        import spark.implicits._

        if(rewriteSchema) rewriteSchema = createCassandraSchema(spark)
        val rdd = tuple.map(t => {
          val modelName = t.substring(0, t.indexOf(":")).trim()
          val jsonStr = t.substring(t.indexOf(":") + 1, t.length).trim()
          println("modelName: " + modelName +  "  " + " jsonStr: " + jsonStr )
          Record(modelName, jsonStr)
        })

        val photos = rdd.filter(r => {
          println("modelName: " + r.modelName +  "  " + " json: " + r.json )
          r.modelName == "photos"
        }).map[Photos](p => Photos.apply(p)).saveToCassandra("test", "photos")

//        val business = rdd.filter(r => {
//          r.modelName == "business"
//        }).map[Business](b => Business.apply(b)).saveToCassandra("test", "business")


        val user = rdd.filter(r => {
          println("modelName: " + r.modelName +  "  " + " json: " + r.json )
          r.modelName == "user"
        }).map[User](u => User.apply(u)).saveToCassandra("test", "user")

        val review = rdd.filter(r => {
          println("modelName: " + r.modelName +  "  " + " json: " + r.json )
          r.modelName == "review"
        }).map[Review](r => Review.apply(r)).saveToCassandra("test", "review")

        val tip = rdd.filter(r => {
          println("modelName: " + r.modelName +  "  " + " json: " + r.json )
          r.modelName == "tip"
        }).map[Tip](r => Tip.apply(r)).saveToCassandra("test", "tip")

        val checkin = rdd.filter(r => {
          println("modelName: " + r.modelName +  "  " + " json: " + r.json )
          r.modelName == "checkin"
        }).flatMap[Checkin](r => Checkin.apply(r))
          checkin.saveToCassandra("test", "checkin")

      })
    streaming.start()
    dataSocketSender.start()
    ssc.start()
    unCompresser.start()
    dataSocketSender.join()
    unCompresser.join()
    println("WE ARE DONE")
    ContextKeeper.ssc.stop(true, true)
  }

case class Record(modelName: String, json: String)

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
