import java.io.{IOException, InputStreamReader, BufferedReader}
import java.net.{MalformedURLException, URLConnection, URL}
import java.text.SimpleDateFormat

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.spark.streaming._
import org.apache.spark.streaming.StreamingContext._
import org.apache.spark.streaming.kafka._

import com.datastax.spark.connector._
import com.datastax.spark.connector.streaming._
import com.datastax.spark.connector.cql.CassandraConnector


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

  //method for finding out IP's location. TODO gut JSON.parseFull for a better parsing lib that produces types (argonaut)
  def resolveIp(ip: String): (String, String) = {
    val url = "http://api.hostip.info/get_json.php"
    var bufferedReader: BufferedReader = null
    try {
      val geoURL = new URL(url + "?ip=" + ip)
      val connection: URLConnection = geoURL.openConnection()
      bufferedReader = new BufferedReader(new InputStreamReader(connection.getInputStream))
      val parsedResult = JSON.parseFull(bufferedReader.readLine())
      parsedResult match {
        //case Some(map: Map[String, String]) => (map("country_name"), map("city"))
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

  val dateFormat = new SimpleDateFormat("dd/MMM/yyyy:HH:mm:ss Z")

  def main(args: Array[String]) {

    val topics = "apache"
    val numThreads = 2
    val zkQuorum = "localhost:2181"   // Zookeeper quorum (hostname:port,hostname:port)
    val clientGroup = "sparkFetcher"

    val logFile = "/home/plamb/Coding/Web_Analytics_POC/logtest/logtest.data" // Should be some file on your system
    val conf = new SparkConf(true)
        .setAppName("sparktry")
        .setMaster("local[*]")
        .set("spark.cassandra.connection.host", "127.0.0.1")
        .set("spark.executor.extraClassPath", "/home/plamb/Coding/Web_Analytics_POC/spark-cassandra-connector/spark-cassandra-connector/target/scala-2.10/spark-cassandra-connector-assembly-1.1.1-SNAPSHOT.jar")

    CassandraConnector(conf).withSessionDo { session =>
      session.execute(s"DROP KEYSPACE IF EXISTS ipAddresses")
      session.execute(s"CREATE KEYSPACE IF NOT EXISTS ipAddresses WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': 1 }")
      session.execute(s"CREATE TABLE ipAddresses.timeOnPage (IP text PRIMARY KEY, page map<text, bigint>)")
    }
    val sc = new StreamingContext(conf, Seconds(2))
    //SparkContext for batch

    // assign equal threads to process each kafka topic
    val topicMap = topics.split(",").map((_, numThreads.toInt)).toMap

    // create an input stream that pulls messages from a kafka broker
    val events = KafkaUtils.createStream(
      sc,            // StreamingContext object
      zkQuorum,
      clientGroup,
      topicMap  // Map of (topic_name -> numPartitions) to consume, each partition is consumed in its own thread
      // StorageLevel.MEMORY_ONLY_SER_2 // Storage level to use for storing the received objects
    ).map(_._2)

//    val logData = sc.textFile(logFile, 2).cache()
//
//    val logEvents = logData
//      .flatMap(_.split("\n"))
//      .map(parseLogEvent)

    val logEvents = events
      .flatMap(_.split("\n")) // take each line of DStream
      .map(parseLogEvent) // parse that to log event

    //val geolocation = logEvents.take(5).map(event => resolveIp(event.ip))

    //geolocation.foreach(println)

// convert the parsed log into ip address, start time, start time, page requested
    val ipTimeStamp = logEvents.map[(String, (String, Long, Long))](event => {
      val time = dateFormat.parse(event.timestamp).getTime()

      (event.ip, (event.requestPage, time, time))
    })



    //This code groups all the pages hit + their timestamps by each IP address resulting in (IP, CollectionBuffer) then
    //applies a map function to the elements of the CollectionBuffer (mapValues) that groups them by the pages that were hit (groupBy), then
    //it filters out all the assets requested we don't care about (filterKeys) (images css files etc) and then it maps a function to
    // the values of the groupBy (i.e. the List of (page visited, timestamp timestamp) using foldLeft to reduce them to one session so as to
    //see the time spent on each page by each IP. TODO: break the mapValues/filterkeys calls into their own functions
      val grouped = ipTimeStamp
                    .groupByKey() //ipAdress, array of requested page/timestamps
                    .mapValues { a => {
      // for everything in the above array, group it by the requested Page, results in (ipAddress, Map(page -> List((page, time, time)...)...) TODO: needs error handling
      a.groupBy(_._1)
        .filterKeys {
        a => {
          a.endsWith(".html") || a.endsWith(".php") || a.equals("/") //filter on requests we care about
        }
      }
        .mapValues {
        //apply a function to the List of page + timestamps for each page
        case Nil => None;
        case (_, a, b) :: tail => //ignore the page String so we can return a (Long, Long)
          Some(x = tail.foldLeft((a, b)) {
            // Apply the foldLeft to each of the times, finding the min time and the max time for start/end
            case ((startTime, nextStartTime), (_, endTime, nextEndTime)) => (startTime min nextStartTime, endTime max nextEndTime)
          })
            .map{
            case (firstTime, lastTime) => lastTime - firstTime}
          //this finds the total length of the session
        }
      }
    }


    grouped.saveToCassandra("ipaddresses", "timeonpage", SomeColumns("ip", "page"))



        }
      }

