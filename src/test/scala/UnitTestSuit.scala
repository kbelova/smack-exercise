import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.junit._
import org.scalatest.junit.AssertionsForJUnit

class UnitTestSuit extends AssertionsForJUnit {
  var spark: SparkSession = _
  val conf = ConfigFactory.load()
  var ssc: StreamingContext = _

  @Before
  def initialize() {
    spark = SparkSession.builder()
      .appName("Test run of " + conf.getString("spark.appname"))
      .master("local[2]")
      .getOrCreate()
    ssc = new StreamingContext(spark.sparkContext, Seconds(1))
  }

  @After
  def tearDown() {
    spark.stop()
  }

  protected def getResourceFilePath(fileName: String): String = {
    getClass.getResource(fileName).getPath
  }
}
