import org.junit.Test
import smackexercise.uncompress.RecordsBuffer

class ArchiveHelperTest extends UnitTestSuit{

  @Test
  def getEntryNameTest(): Unit = {
    assertResult("photos")(RecordsBuffer.getEntryName("input/json/photos.json"))
    assertResult("photos")(RecordsBuffer.getEntryName("input/someotherdir/photos.json"))

  }


}
