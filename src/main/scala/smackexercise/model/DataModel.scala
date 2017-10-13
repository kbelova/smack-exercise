package smackexercise.model

import java.util.UUID.randomUUID

import org.apache.spark.sql.Row
import org.json4s.DefaultFormats
import org.json4s.jackson.JsonMethods

import scala.collection.mutable
import smackexercise.utils.Transformer

import scala.collection.mutable.ArrayBuffer

  case class Checkin(checkin_id: String, business_id: String, day: String, hour: String, amount: BigInt) extends Product
  object Checkin {
    def apply(record: Record): mutable.ArrayBuffer[Checkin] = {
      extract(record.json)
    }

    def extract(jsonStr: String): ArrayBuffer[Checkin] = {
      implicit val formats: DefaultFormats.type = DefaultFormats
      val json = JsonMethods.parse(jsonStr).extract[Map[String, Any]]
      val businessId = json("business_id").asInstanceOf[String]
      val days = json("time").asInstanceOf[Map[String, Map[String, BigInt]]]
      val lst = new mutable.ArrayBuffer[Checkin]()
      for ((day: String, hours: Map[String, BigInt]) <- days) {
        for ((hour: String, amount: BigInt) <- hours) {
          lst += new Checkin(randomUUID().toString, businessId, day, hour, amount)
        }
      }
      lst
    }
  }

  case class Review(review_id: String, user_id: String, business_id: String, stars: Option[BigInt], `date`: String, `text`: String, useful: BigInt, funny: BigInt, cool: BigInt) extends Product
  object Review {
    def apply(record: Record): Review = {
      extract(record.json)
    }

    def extract(jsonStr: String): Review = {
      implicit val formats = DefaultFormats
      val json = JsonMethods.parse(jsonStr)
      json.extract[Review]
    }
  }

  case class SmallTip(`text`: Option[String], `date`: Option[String], likes: Option[BigInt], business_id: String, user_id: String) extends Product
  case class Tip(tip_id: String,`text`: String, `date`: String, likes: BigInt, business_id: String, user_id: String) extends Product
  object Tip {
    def apply(smTip: SmallTip): Tip = Tip(
      tip_id = randomUUID().toString,
      `text` = smTip.`text`.getOrElse("None"),
      `date` = smTip.`date`.getOrElse("1111-11-11"),
      likes = smTip.likes.getOrElse(-1),
      business_id = smTip.business_id,
      user_id = smTip.user_id)
    def apply(record: Record): Tip = {
      val tip: SmallTip = extract(record.json)
      Tip.apply(tip)
    }

    def extract(jsonStr: String): SmallTip = {
      implicit val formats = DefaultFormats
      val json = JsonMethods.parse(jsonStr)
      json.extract[SmallTip]
    }
  }

 case class User(user_id: String, name: String, review_count: BigInt, yelping_since: String, friends: String, useful: BigInt, funny: BigInt, cool: BigInt, fans: BigInt, elite: String, average_stars: Double, compliment_hot: BigInt, compliment_more: BigInt, compliment_profile: BigInt, compliment_cute: BigInt, compliment_list: BigInt, compliment_note: BigInt, compliment_plain: BigInt, compliment_cool: BigInt, compliment_funny: BigInt, compliment_writer: BigInt, compliment_photos: BigInt) extends Product

  case class SmallUser(user_id: String, name: Option[String], review_count:Option[BigInt], yelping_since: Option[String], useful: Option[BigInt], funny: Option[BigInt], cool: Option[BigInt], fans: Option[BigInt], average_stars: Option[Double], compliment_hot: Option[BigInt], compliment_more: Option[ BigInt], compliment_profile: Option[BigInt], compliment_cute: Option[BigInt], compliment_list: Option[BigInt], compliment_note: Option[BigInt], compliment_plain: Option[BigInt], compliment_cool: Option[BigInt], compliment_funny: Option[BigInt], compliment_writer: Option[BigInt], compliment_photos: Option[BigInt])
  case class Friends(friends: List[String]) extends Product
  case class Elite(elite: List[BigInt]) extends Product

  object User {
    def apply(record: Record): User = {
      implicit val formats = DefaultFormats
      val smallUser = JsonMethods.parse(record.json).extract[SmallUser]
      val sf = JsonMethods.parse(record.json).extract[Friends]
      val se = JsonMethods.parse(record.json).extract[Elite]
      User(smallUser, sf.friends.mkString(","), se.elite.mkString(","))
    }
    def apply(sm: SmallUser, friends: String, elite: String): User = {
      User(sm.user_id, sm.name.getOrElse("None"), sm.review_count.getOrElse(-1), sm.yelping_since.getOrElse("None"), friends, sm.useful.getOrElse(-1), sm.funny.getOrElse(-1), sm.cool.getOrElse(-1), sm.fans.getOrElse(-1), elite, sm.average_stars.getOrElse(Double.NaN), sm.compliment_hot.getOrElse(-1), sm.compliment_more.getOrElse(-1), sm.compliment_profile.getOrElse(-1), sm.compliment_cute.getOrElse(-1),sm.compliment_list.getOrElse(-1), sm.compliment_note.getOrElse(-1), sm.compliment_plain.getOrElse(-1), sm.compliment_cool.getOrElse(-1), sm.compliment_funny.getOrElse(-1), sm.compliment_writer.getOrElse(-1), sm.compliment_photos.getOrElse(-1))
    }
  }

  case class TinyBusiness(business_id: String, name: String, neighborhood: Option[String], address: Option[String], city: Option[String], state: Option[String], postal_code: Option[String], latitude: Option[Double], longitude: Option[Double], stars: Option[Double], review_count: Option[BigInt], is_open: Option[BigInt]) extends Product
  case class ComplMap(attributes: Map[String, Any], hours: Map[String, Any], categories: List[String]) {
    def getFullMap: Map[String, Any] = {
      (Transformer.flatter(attributes, mutable.Map[String, Any]()) ++
        Transformer.flatter(hours, mutable.Map[String, Any]()) +
        ("categories" -> categories.mkString(","))).toMap
    }
  }

  case class Business( business_id: String, name: String, neighborhood: String,
                       address: String, city: String, state: String, postal_code: String,
                       latitude: Double, longitude: Double, stars: Double,
                       review_count: BigInt, is_open: BigInt,
                       businessacceptscreditcards: Boolean, restaurantspricerange2: BigInt, wifi: String,
                      `businessparking_garage`: Boolean, `businessparking_street`: Boolean,
                      `businessparking_validated`: Boolean, `businessparking_lot`: Boolean,
                      `businessparking_valet`: Boolean, bikeparking: Boolean,
                       categories: String, monday: String, tuesday: String,
                       wednesday: String, thursday: String,
                       friday: String, saturday: String, sunday: String
                     ) extends Product {
    def this(tb: TinyBusiness, m: Map[String, Any]) =
      this(
        business_id = tb.business_id,
        name = tb.name,
        neighborhood = tb.neighborhood.getOrElse("None"),
        address = tb.address.getOrElse("None"),
        city = tb.city.getOrElse("None"),
        state = tb.state.getOrElse("None"),
        postal_code = tb.postal_code.getOrElse("None"),
        latitude = tb.latitude.getOrElse(Double.NaN),
        longitude = tb.longitude.getOrElse(Double.NaN),
        stars = tb.stars.getOrElse(Double.NaN),
        review_count = tb.review_count.getOrElse(-1),
        is_open = tb.is_open.getOrElse(-1),
        businessacceptscreditcards = m.getOrElse("businessacceptscreditcards", null).asInstanceOf[Boolean],
        restaurantspricerange2 = m.getOrElse("restaurantspricerange2", null).asInstanceOf[BigInt],
        wifi = m.getOrElse("wifi", "").asInstanceOf[String],
        `businessparking_garage` = m.getOrElse("`businessparking_garage`", null).asInstanceOf[Boolean],
        `businessparking_street` = m.getOrElse("`businessparking_street`", null).asInstanceOf[Boolean],
        `businessparking_validated` = m.getOrElse("`businessparking_validated`", null).asInstanceOf[Boolean],
        `businessparking_lot` = m.getOrElse("`businessparking_lot`", null).asInstanceOf[Boolean],
        `businessparking_valet` = m.getOrElse("`businessparking_valet`", null).asInstanceOf[Boolean],
        bikeparking = m.getOrElse("bikeparking", null).asInstanceOf[Boolean],
        monday = m.getOrElse("monday", "").asInstanceOf[String],
        tuesday = m.getOrElse("tuesday", "").asInstanceOf[String],
        wednesday = m.getOrElse("wednesday", "").asInstanceOf[String],
        thursday = m.getOrElse("thursday", "").asInstanceOf[String],
        friday = m.getOrElse("friday", "").asInstanceOf[String],
        saturday = m.getOrElse("saturday", "").asInstanceOf[String],
        sunday = m.getOrElse("sunday", "").asInstanceOf[String],
        categories = m.getOrElse("categories", "").asInstanceOf[String])
  }

  object Business {
    def apply(record: Record): Business = {
      extract(record.json)
    }

    def extract(jsonStr: String): Business = {
      implicit val formats: DefaultFormats.type = DefaultFormats
      val json = JsonMethods.parse(jsonStr)
      val sm = json.extract[TinyBusiness]
      val cs = json.extract[ComplMap]
      new Business(sm, cs.getFullMap)
    }
  }


  case class Photos(business_id: String, photo_id: String,  caption: Option[String], label: Option[String]) extends Product
  object Photos {
    def apply(record: Record): Photos = {
      if (record.json == null || record.json.isEmpty) println("PHOTOS: json is empty or null")
      extract(record.json)
    }

    //for test purpose can't be private
    def extract(jsonStr: String): Photos = {
      implicit val formats: DefaultFormats.type = DefaultFormats
      val json = JsonMethods.parse(jsonStr)
      json.extract[Photos]
    }
  }

  case class Record(modelName: String, json: String) extends Product {
    def isMatch(name: String): Boolean = {
      modelName == name
    }
  }
  object Record {
    def apply(t: Row): Record = {
      Record(t.getString(0))
    }
    def apply(unparsed: String): Record = {
      val name = unparsed.substring(0, unparsed.indexOf(":")).trim()
      val json = unparsed.substring(unparsed.indexOf(":") + 1, unparsed.length).trim()
      new Record(name, json)
    }
}
