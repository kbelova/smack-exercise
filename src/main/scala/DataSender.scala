import java.util.concurrent.{LinkedBlockingQueue, TimeUnit}
import java.net.ServerSocket
import java.io._

import com.typesafe.config.ConfigFactory


object DataSender extends Runnable {
  val appConfig = ConfigFactory.load()
  val server = new ServerSocket(appConfig.getInt("spark.streaming.port"))
  var continueRun = true

  override def run(): Unit = {
    while (continueRun) {
      val s = server.accept()
//      println("connected")
      val out = new PrintStream(s.getOutputStream())
      try {
        while(continueRun) {
          val msg = RecordsQueue.queue.poll(1, TimeUnit.SECONDS)
//          println(RecordsQueue.toString)
//          println(msg)
          if(msg != null) {
            out.println(msg)
          } else {
            if (RecordsQueue.finish) continueRun = false
          }
        }
      } finally {
        out.flush()
        println("datasender all done")
        s.close()
      }
    }
  }
}

object RecordsQueue {
  @volatile var finish = false
  val queue: LinkedBlockingQueue[String] = new LinkedBlockingQueue[String](1000)
  def put(msg: (String)) = {
    queue.put(msg)
  }
}
