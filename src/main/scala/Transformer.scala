package smackexercise.utils {
  import scala.collection.mutable

  object Transformer {
    def flatter(o: Map[String, Any], acc: mutable.Map[String, Any], prefix: String = ""): mutable.Map[String, Any] = {

      for ((key, value) <- o) {
        value match {
          case m: Map[String, Any] =>
            flatter(m, acc, key)
          case _ =>
            acc.put(buildKey(prefix, key), value)
        }
      }
      acc

    }

    def buildKey(prefix: String, key: String): String = {
      if (!prefix.isEmpty) {
        "`" + prefix.toLowerCase + "_" + key.toLowerCase + '`'
      } else
        key.toLowerCase()
    }
  }
}
