import org.junit.Test

class DataModelTests extends UnitTestSuit {

  final val path: String = "/input/stream/"

  @Test
  def modelCaseClassesTest(): Unit = {
    val sparkTestLocal = spark
    import sparkTestLocal.implicits._

//    val rdd = ssc.textFileStream(getResourceFilePath(path + "user"))
//    val b = rdd.map(t => Record(t))
//    .map[User](b => User.apply(b))
//    val size = b.count()
//    val bs = b.foreachRDD(value => {
//      println("im inside is streaming" + ssc.getState())
//      val users = value.collect()
//      val size = value.count()
//      println("did users collect: " + size + " users.size = " + users.length)
//
//      users.foreach(u => {
//        println("im iterating")
//        println(u.toString)
//        assertResult("")(u.user_id)
//        assertResult("")(u.elite)
//      })
//    })
//    ssc.start()
//
//    ssc.awaitTermination()
 }

}
