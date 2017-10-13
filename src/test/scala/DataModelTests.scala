import org.apache.spark.rdd.RDD
import org.junit.Test
import smackexercise.model._
import smackexercise.model.SourceEnum._

class DataModelTests extends UnitTestSuit {
  final private val business1 = "business1"
  final private val business2 = "business2"
  final private val photo1 = "photo1"
  final private val photo2 = "photo2"
  final private val photo3 = "photo3"
  final private val photo4 = "photo4"
  final private val photo5 = "photo5"
  final private val address1 = "address 1"
  final private val postalCode = "00000"
  final private val stars: Double = 3.5
  final private val user1 = "user1"
  final private val user2 = "user2"
  final private val user3 = "user3"
  final private val review1 = "review1"
  final private val review2 = "review2"
  final private val text = "text1"
  final private val date = "2000-02-02"
  final private val name = "name1"

  def path2json(modelName: String): String = s"/input/json/${modelName}.json"

  @Test
  def modelPhotosTest{
    val sparkTestLocal = spark
    import sparkTestLocal.implicits._

    val jsonFileName = path2json(PHOTOS.name)
    val rdd: RDD[String] = sparkTestLocal.sparkContext.textFile(getResourceFilePath(jsonFileName))

    val photos: Array[Photos] =  rdd.map(r => Photos.extract(r)).collect()
    assertResult(expectedPhotos._1)(photos(0))
    assertResult(expectedPhotos._2)(photos(1))
    assertResult(expectedPhotos._3)(photos(2))
    assertResult(expectedPhotos._4)(photos(3))
    assertResult(expectedPhotos._5)(photos(4))

 }
  val expectedPhotos = (
    new Photos(caption=Option("caption1"), label=Option("label1"),  photo_id = photo1, business_id=business1),
    new Photos(business_id = business1, photo_id=photo2, caption = Option(""), label = Option("label2")),
    new Photos(business_id = business1, photo_id=photo3, caption = Option(""), label = Option("")),
    new Photos(business_id = business2, photo_id=photo4, caption = None, label = Option("food")),
    new Photos(business_id = business2, photo_id=photo5, caption = None, label = None)
  )

  @Test
  def modelBusinessTest {
    val sparkTestLocal = spark
    import sparkTestLocal.implicits._
    val jsonFileName = path2json(BUSINESS.name)
    val rdd: RDD[String] = sparkTestLocal.sparkContext.textFile(getResourceFilePath(jsonFileName))

    val business: Array[Business] =  rdd.map(s => Business.extract(s)).collect()
    assertResult(1)(business.length)
    assertResult(expectedBusiness)(business(0))
  }
  val expectedBusiness =
    new Business(business_id = business1, name = "name1", neighborhood = "nei1", address = address1, city = "city1", state = "st", postal_code = postalCode, latitude = 43.0, longitude = -89.3, stars = stars, review_count = 5, is_open = 1, businessacceptscreditcards = false, restaurantspricerange2 = 1, wifi = "free", `businessparking_garage` = false, `businessparking_street` = false, `businessparking_validated` = false, `businessparking_lot` = true, `businessparking_valet` = false, bikeparking = true, categories = "cat1,cat2", monday = "monday", tuesday = "tuesday", wednesday = "wednesday", thursday = "thursday", friday = "friday", saturday = "saturday", sunday = "sunday")

  @Test
  def modelCheckinTest {
    val sparkTestLocal = spark
    import sparkTestLocal.implicits._
    val jsonFileName = path2json(CHECKIN.name)
    val rdd: RDD[String] = sparkTestLocal.sparkContext.textFile(getResourceFilePath(jsonFileName))

    val checkin: Array[Checkin] = rdd.flatMap(s => Checkin.extract(s)).collect()

    assertResult(3)(checkin.length)
    assertResult(business1)(checkin(0).business_id)
    assertResult("Thursday")(checkin(0).day)
    assertResult("21:00")(checkin(0).hour)
    assertResult(4)(checkin(0).amount)
  }

  @Test
  def modelReviewTest {
    val sparkTestLocal = spark
    import sparkTestLocal.implicits._
    val jsonFileName = path2json(REVIEW.name)
    val rdd: RDD[String] = sparkTestLocal.sparkContext.textFile(getResourceFilePath(jsonFileName))

    val review: Array[Review] = rdd.map(s => Review.extract(s)).collect()

    assertResult(2)(review.length)
    assertResult(expectedReviews._1)(review(0))
    assertResult(expectedReviews._2)(review(1))

  }
  val expectedReviews: (Review, Review) = (
    new Review(review_id = review1, user_id = user1, business_id = business1, stars = Option(BigInt.int2bigInt(4)) , `date` = date, `text` = text, useful = 0, funny = 0, cool = 0),
    new Review(review_id = review2, user_id = user2, business_id = business1, stars = None, `date` = date, text = "test\n\ntest", useful = 1, funny=2, cool = 3)
  )

  @Test
  def modelSmallTipTest {
    val sparkTestLocal = spark
    import sparkTestLocal.implicits._
    val jsonFileName = path2json(TIP.name)
    val rdd: RDD[String] = sparkTestLocal.sparkContext.textFile(getResourceFilePath(jsonFileName))

    val tip: Array[SmallTip] = rdd.map(s => Tip.extract(s)).collect()

    assertResult(1)(tip.length)
    assertResult(business1)(tip(0).business_id)
    assertResult(user1)(tip(0).user_id)
    assertResult(Some(text))(tip(0).`text`)
    assertResult(Some(date))(tip(0).`date`)
    assertResult(Some(0))(tip(0).likes)

  }

  @Test
  def modelUserTest {
    val sparkTestLocal = spark
    import sparkTestLocal.implicits._
    val jsonFileName = path2json(USER.name)
    val rdd: RDD[String] = sparkTestLocal.sparkContext.textFile(getResourceFilePath(jsonFileName))

    val user: Array[User] = rdd.map(s => User.apply(new Record(USER.name, s))).collect()

    assertResult(1)(user.length)
    assertResult(expectedUser)(user(0))
  }

  val expectedUser = new User(user_id = user1, name = name, review_count = 1, yelping_since = date, friends = user2 + "," + user3, useful = 2, funny = 3, cool = 4, fans = 5, elite = "2012,2013", average_stars = 4.31, compliment_hot = 6, compliment_more = 7, compliment_profile = 8, compliment_cute = 9, compliment_list = 10, compliment_note = 11, compliment_plain = 12, compliment_cool = 13, compliment_funny = 14, compliment_writer = 15, compliment_photos = 16)

}
