package smackexercise.model

import enumeratum._

sealed abstract class SourceFiles(val name: String) extends EnumEntry {
}

case object SourceEnum extends Enum[SourceFiles]{
  val values = findValues

  case object PHOTOS extends SourceFiles("photos")
  case object BUSINESS extends SourceFiles("business")
  case object CHECKIN extends SourceFiles("checkin")
  case object REVIEW extends SourceFiles("review")
  case object TIP extends SourceFiles("tip")
  case object USER extends SourceFiles("user")
}










