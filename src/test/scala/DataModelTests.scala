
import model.Business
import org.junit.Test

class DataModelTests extends UnitTestSuit {

  @Test
  def modelCaseClassesTest(): Unit = {
    val sparkTestLocal = spark
    import sparkTestLocal.implicits._
      val df = sparkTestLocal.sqlContext.read.json(getResourceFilePath("/input/" + "business.json")).as[Business]
//      println(f)
      df.show()
      df.schema
      df.printSchema()

 }

}
