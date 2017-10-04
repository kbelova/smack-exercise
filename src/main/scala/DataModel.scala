import org.apache.spark.sql.types._
import org.json4s.DefaultFormats
import org.json4s._
import org.json4s.jackson._
import org.json4s.jackson.JsonMethods._
import java.util.UUID.randomUUID

import org.json4s.jackson.JsonMethods

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

object model {

  abstract class DModel {
    var modelName: String = ""
    def modelClass = this.getClass
    def selectToTsv = s"SELECT * FROM $modelName"
    def schema: StructType = null
  }

  object DataModel {
    def apply(name: String): DModel = {
      name match {
        case "business" => Business
        case "photos" => Photos
        case "checkin" => Checkin
        case "review" => Review
        case "tip" => Tip
        case "user" => User
        //TODO case _
      }
    }
  }

  case class Checkin(busines_id: String, day: String, hour: String, amount: BigInt) extends Product {
//    def apply(record: App.Record): ArrayBuffer[Checkin]
  }
  object Checkin extends DModel {
    modelName = "checkin"
    def apply(record: App.Record): mutable.HashSet[Checkin] = {
      implicit val formats = DefaultFormats
      val json = JsonMethods.parse(record.json).extract[Map[String, Any]]
      val businessId = json("business_id"). asInstanceOf[String]
      val days = json("time").asInstanceOf[Map[String, Map[String, BigInt]]]
      val lst = new mutable.HashSet[Checkin]()
      for ((day:String, hours: Map[String, BigInt] ) <- days) {
        for((hour: String, amount: BigInt) <- hours) {
          lst += new Checkin(businessId, day, hour, amount)
        }
      }
      lst
    }
    override def selectToTsv: String = "Select business_id, day, time, amount FROM checkin"
  }

  case class Review(review_id: String, user_id: String, business_id: String, stars: BigInt, `date`: String, `text`: String, useful: BigInt, funny: BigInt, cool: BigInt) extends Product
  object Review extends DModel {
    modelName = "review"
    def apply(record: App.Record): Review = {
      implicit val formats = DefaultFormats
      val json = JsonMethods.parse(record.json)
      json.extract[Review]
    }
  }

  case class Tip(`text`: String, `date`: String, likes: BigInt, business_id: String, user_id: String) extends Product
  object Tip extends DModel {
    modelName = "tip"
    def apply(record: App.Record): Tip = {
      implicit val formats = DefaultFormats
      val json = JsonMethods.parse(record.json)
      json.extract[Tip]
    }
  }

  //TIP better to move friends list into a separate table with 1 to 1 connection, as a base for social graph (in case we would need it in research)
  case class User(user_id: String, name: String, review_count: BigInt, yelping_since: String, friends: String, useful: BigInt, funny: BigInt, cool: BigInt, fans: BigInt, elite: String, average_stars: Double, compliment_hot: BigInt, compliment_more: BigInt, compliment_profile: BigInt, compliment_cute: BigInt, compliment_list: BigInt, compliment_note: BigInt, compliment_plain: BigInt, compliment_cool: BigInt, compliment_funny: BigInt, compliment_writer: BigInt, compliment_photos: BigInt) extends Product

  object User extends DModel {
    def apply(record: App.Record): User = {
      implicit val formats = DefaultFormats
      val json = JsonMethods.parse(record.json).extract[Map[String, Any]]
//FIXME here automated needed
      val userId = json.getOrElse("user_id",null).toString
      val name = json.getOrElse("name", null).toString
      val reviewCount = json.getOrElse("review_count",null).asInstanceOf[BigInt]
      val yelpingSince = json.getOrElse("yelping_since", null).toString
      val friends = json("friends").asInstanceOf[List[String]].mkString(" ")
      val useful = json.getOrElse("useful", null).asInstanceOf[BigInt]
      val funny = json.getOrElse("funny", null).asInstanceOf[BigInt]
      val cool = json.getOrElse("cool", null).asInstanceOf[BigInt]
      val fans = json.getOrElse("fans", null).asInstanceOf[BigInt]
      val averageStars = json.getOrElse("average_stars", null).asInstanceOf[Double]
      val elite = json("elite").asInstanceOf[List[BigInt]].mkString(" ")
      val complimentHot = json.getOrElse("compliment_hot", null).asInstanceOf[BigInt]
      val complimentMore = json.getOrElse("compliment_more", null).asInstanceOf[BigInt]
      val complimentProfile = json.getOrElse("compliment_profile", null).asInstanceOf[BigInt]
      val complimentCute = json.getOrElse("compliment_cute", null).asInstanceOf[BigInt]
      val complimentList = json.getOrElse("compliment_list", null).asInstanceOf[BigInt]
      val compliment_note = json.getOrElse("compliment_note", null).asInstanceOf[BigInt]
      val compliment_plain = json.getOrElse("compliment_plain", null).asInstanceOf[BigInt]
      val compliment_cool = json.getOrElse("compliment_cool", null).asInstanceOf[BigInt]
      val compliment_funny = json.getOrElse("compliment_funny", null).asInstanceOf[BigInt]
      val compliment_writer = json.getOrElse("compliment_writer", null).asInstanceOf[BigInt]
      val compliment_photos = json.get("compliment_photos").orNull


      new User(userId, name, reviewCount, yelpingSince, friends, useful, funny, cool, fans, elite,averageStars, complimentHot, complimentMore, complimentProfile, complimentCute, complimentList, compliment_note, compliment_plain, compliment_cool, compliment_funny, compliment_writer, compliment_photos.asInstanceOf[BigInt])
    }
    modelName = "user"
    override def selectToTsv = "SELECT user_id, name, review_count, yelping_since, stringify(friends), useful, funny, cool, fans, stringify(elite), average_stars, compliment_hot,compliment_more, compliment_profile, compliment_cute, compliment_list, compliment_note, compliment_plain, compliment_cool, compliment_funny, compliment_writer, compliment_photos FROM user"
  }
  case class SmallerBusiness(business_id: String, name: String, neighborhood: String, address: String, city: String, state: String, postal_code: String, latitude: Double, longitude: Double, stars: Double, review_count: BigInt, is_open: BigInt) extends Product

  case class Business(business_id: String, name: String, neighborhood: String, address: String, city: String, state: String, postal_code: String, latitude: Double, longitude: Double, stars: Double, review_count: BigInt, is_open: BigInt, categories: String) extends Product
  object Business extends DModel {
    modelName = "business"

    override def selectToTsv = "SELECT business_id, name, address, neighborhood, city, state, postal_code, businessacceptscreditcards as is_accepts_credit_cards, businessparking as business_parking, bikeparking as bike_parking, restaurantspricerange2 as restaurant_price_range2, wifi as wifi, stringify(categories), monday as open_monday, tuesday as open_tuesday, wednesday as open_wednesday, thursday as open_thursday, friday as open_friday, saturday as open_saturday, sunday as open_sunday, is_open, latitude, longitude, review_count, stars FROM business"

    override def schema = StructType(Seq(
      StructField("business_id", StringType, true)
      , StructField("name", StringType, true)
      , StructField("neighborhood", StringType, true)
      , StructField("address", StringType, true)
      , StructField("city", StringType, true)
      , StructField("state", StringType, true)
      , StructField("postal_code", StringType, true)
      , StructField("latitude", DoubleType, true)
      , StructField("longitude", DoubleType, true)
      , StructField("stars", DoubleType, true)
      , StructField("review_count", LongType, true)
      , StructField("is_open", LongType, true)
      , StructField("attributes", StructType(Seq(
           StructField("BusinessAcceptsCreditCards", BooleanType, true)
          , StructField("RestaurantsPriceRange2", LongType, true)
          , StructField("WiFi", StringType, true)
      , StructField("BusinessParking", StructType(Seq( StructField("garage", BooleanType, true)
                        , StructField("street", BooleanType, true)
                        , StructField("validated", BooleanType, true)
                        , StructField("lot", BooleanType, true)
                        , StructField("valet", BooleanType, true))), true)
      , StructField("BikeParking", BooleanType, true))), true)
      , StructField("categories", ArrayType(StringType, true),true)
        , StructField("hours",StructType(Seq(
          StructField("Friday", StringType, true),
          StructField("Monday", StringType, true),
          StructField("Saturday", StringType, true),
          StructField("Sunday", StringType, true),
          StructField("Thursday", StringType, true),
          StructField("Tuesday", StringType, true),
          StructField("Wednesday", StringType, true))), true)))




    def apply(record: App.Record): Business = {
      implicit val formats = DefaultFormats
      val json = JsonMethods.parse(record.json)

      json.extract[Business]
    }
  }

  case class Photos(photo_id: String, business_id: String, caption: String, label: String) extends Product
  object Photos extends DModel {
    def apply(record: App.Record): Photos = {
      implicit val formats: DefaultFormats.type = DefaultFormats
      val json = JsonMethods.parse(record.json)
      json.extract[Photos]
    }
    modelName = "photos"
    override def selectToTsv = "Select photo_id, business_id, caption, label FROM photos"
    override def schema = StructType(Seq(StructField("photo_id", StringType, true), StructField("business_id", StringType, true), StructField("caption", StringType, true), StructField("label", StringType, true)))
  }

}
