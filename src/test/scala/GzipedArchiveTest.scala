import org.apache.spark.sql.catalyst.expressions.AssertTrue
import org.junit.Test

class GzipedArchiveTest extends UnitTestSuit{

  @Test
  def getEntryNameTest(): Unit = {
    assertResult("photos")(RecordsBuffer.getEntryName("input/photos.json"))
    assertResult("photos")(RecordsBuffer.getEntryName("input/someotherdir/photos.json"))

  }


}
