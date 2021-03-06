package smackexercise.uncompress

import java.io
import java.io.FileInputStream
import java.nio.charset._

import com.typesafe.config.ConfigFactory
import org.apache.commons.compress.archivers.tar.{TarArchiveEntry, TarArchiveInputStream}
import org.apache.commons.compress.compressors.gzip.GzipCompressorInputStream

  class ArchiveHelper(tarball: String) {
    val config = ConfigFactory.load()
    var tar: TarArchiveInputStream = init()
    var bufferSize: Int = 1024

    def init(): TarArchiveInputStream = {
      val in: io.InputStream = new FileInputStream(tarball)
      new TarArchiveInputStream(new GzipCompressorInputStream(in))
    } //TODO check open in and close it? (TRY)

    def processArchive() {
      Stream.continually(Option(tar.getNextTarEntry))
        .takeWhile(_.isDefined).flatten
        .filter(!_.isDirectory)
        .foreach(e => processEntry(e))
      RecordsQueue.finish = true
    }

    def processEntry(entry: TarArchiveEntry): Unit = {
      val name = RecordsBuffer.getEntryName(entry.getName)
      Stream.continually {
        // Read n bytes
        val buffer = Array.fill[Byte](bufferSize)(-1)
        val i = tar.read(buffer, 0, bufferSize)
        (i, buffer.take(i))
      }.takeWhile(t => t._1 > 0) //while we have data in that entry flow
        .foldLeft("")((acc, tuple) => {
        RecordsBuffer.extractAndSend(acc + RecordsBuffer.decode()(tuple._2), name)
      })
    }
  }

  object RecordsBuffer {
    val ending = "}\n"

    def decode(charset: Charset = StandardCharsets.UTF_8)(bytes: Array[Byte]) = new String(bytes, StandardCharsets.UTF_8)

    def extractAndSend(line: String, modelName: String): String = {
      val msgAmount = countOccurrence(line)

      if (msgAmount == 0) return line //no complete records in buffer

      val splits = line.split(ending)
      (0 to(msgAmount - 1, 1)).foreach(i => {
        RecordsQueue.put(buildMsg(modelName, splits(i)))
      })

      if (msgAmount != splits.size) splits(splits.size - 1)
      else ""
    }

    def buildMsg(modelName: String, line: String): String = {
      s"$modelName:${line.concat("}")}\\n"
    }

    def getEntryName(str: String): String = {
      val s = str.stripSuffix(".json")
      if (!s.contains('/')) s
      else {
        s.substring(s.lastIndexOf('/') + 1, s.size)
      }
    }

    def countOccurrence(line: String): Int = {
      var lastIndex = 0
      var count = 0
      while (lastIndex != -1) {
        lastIndex = line.indexOf(ending, lastIndex)
        if (lastIndex != -1) {
          count += 1
          lastIndex += ending.length()
        }
      }
      count
    }
  }