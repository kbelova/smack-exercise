import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.SparkSession
import org.junit._
import org.scalatest.junit.AssertionsForJUnit

class UnitTestSuit extends AssertionsForJUnit {
  var spark: SparkSession = _
  val conf = ConfigFactory.load()

  @Before
  def initialize() {
    spark = SparkSession.builder()
      .appName("Test run of " + conf.getString("spark.appname"))
      .master("local")
      .getOrCreate()
  }

  @After
  def tearDown() {
    spark.stop()
  }

  protected def getResourceFilePath(fileName: String): String = {
    getClass.getResource(fileName).getPath
  }
}
