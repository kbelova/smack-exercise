import java.util.concurrent.{LinkedBlockingQueue, TimeUnit}
import java.net.ServerSocket
import java.io._


object DataSender extends Runnable {
  val server = new ServerSocket(9999)
  var continueRun = true

  override def run(): Unit = {
    while (continueRun) {
      val s = server.accept()
      println("connected")
      val out = new PrintStream(s.getOutputStream())
      try {
        while(continueRun) {
          val msg = RecordsQueue.queue.poll(5, TimeUnit.SECONDS)
          println(RecordsQueue.toString)
          println(msg)
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
  val queue: LinkedBlockingQueue[String] = new LinkedBlockingQueue[String]()
  def put(msg: (String)) = {
    queue.put(msg)
  }
}
