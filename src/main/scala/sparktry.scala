import java.io.{IOException, InputStreamReader, BufferedReader}
import java.net.{MalformedURLException, URLConnection, URL}

import org.apache.spark._

import scala.util.parsing.json.JSON

case class LogEvent(ip:String, timestamp:String, requestPage:String, responseCode:Int, responseSize:Int, userAgent:String)

object sparktry {

  //method for parsing the logs passed in
  def parseLogEvent(event: String): LogEvent = {
    val LogPattern = """^([\d.]+) (\S+) (\S+) \[(.*)\] \"([^\s]+) (/[^\s]*) HTTP/[^\s]+\" (\d{3}) (\d+) \"([^\"]+)\" \"([^\"]+)\"$""".r
    val m = LogPattern.findAllIn(event)
    if (m.nonEmpty)
      new LogEvent(m.group(1), m.group(4), m.group(6), m.group(7).toInt, m.group(8).toInt, m.group(10))
    else
      null
  }

  //method for finding out IP's location
  def resolveIp(ip: String): (String, String) = {
    val url = "http://api.hostip.info/get_json.php"
    var bufferedReader: BufferedReader = null
    try {
      val geoURL = new URL(url + "?ip=" + ip)
      val connection: URLConnection = geoURL.openConnection()
      bufferedReader = new BufferedReader(new InputStreamReader(connection.getInputStream))
      val parsedResult = JSON.parseFull(bufferedReader.readLine())
      parsedResult match {
        case Some(map: Map[String, String]) => (map("country_name"), map("city"))
        case None => (null, null) // parsing failed
        case other => (null, null) // unknown data structure
      }
    } catch {
      case e: MalformedURLException => {
        println("Exception caught: " + e.printStackTrace())
        (null, null)
      }
      case e: IOException => {
        println("Exception caught: " + e.printStackTrace())
        (null, null)
      }
    } finally {
      if (bufferedReader != null) {
        try {
          bufferedReader.close()
        } catch {
          case _: Throwable =>
        }
      }
    }
  }


  def main(args: Array[String]) {
    val logFile = "/home/plamb/Coding/Web_Analytics_POC/logtest/logtest.data" // Should be some file on your system
    val conf = new SparkConf()
        .setAppName("sparktry")
        .setMaster("local[*]")
    val sc = new SparkContext(conf)
    val logData = sc.textFile(logFile, 2).cache()

    val logEvents = logData
      .flatMap(_.split("\n"))
      .map(parseLogEvent)


    val numAs = logData.filter(line => line.contains("a")).count()
    val numBs = logData.filter(line => line.contains("b")).count()
    println("Lines with a: %s, Lines with b: %s".format(numAs, numBs))
  }
}