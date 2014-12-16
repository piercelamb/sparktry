name := "SparkTry"

version := "1.0"

scalaVersion := "2.10.4"

libraryDependencies += "org.apache.spark" %% "spark-core" % "1.1.1"

libraryDependencies += "com.datastax.spark" %% "spark-cassandra-connector" % "1.1.0"

libraryDependencies += "org.apache.spark" %% "spark-streaming" % "1.1.1"

libraryDependencies += "org.apache.spark" %% "spark-streaming-kafka" % "1.1.1"