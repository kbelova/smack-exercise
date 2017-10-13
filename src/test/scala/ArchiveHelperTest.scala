import org.junit.Test
import smackexercise.uncompress.RecordsBuffer

class ArchiveHelperTest extends UnitTestSuit{

  @Test
  def getEntryNameTest(): Unit = {
    assertResult("photos")(RecordsBuffer.getEntryName("photos.json"))
    assertResult("photos")(RecordsBuffer.getEntryName("input/someotherdir/photos.json"))

  }

  @Test
  def countOccurrenceTest(): Unit = {
    assertResult(1)(RecordsBuffer.countOccurrence("{eee}\n"))
    assertResult(0)(RecordsBuffer.countOccurrence("{eee"))
    assertResult(3)(RecordsBuffer.countOccurrence("{eee}\n{fweewf}\n{ewwwwww}\nw"))
    assertResult(3)(RecordsBuffer.countOccurrence("{eee}\nfweewfw}\newwwwww}\n"))
    assertResult(1)(RecordsBuffer.countOccurrence("{eee\n\n}\n"))
  }


}
