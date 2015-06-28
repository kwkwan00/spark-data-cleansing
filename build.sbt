name := "spark-data-cleansing"

version := "1.0"

scalaVersion := "2.10.5"

assemblyJarName in assembly := "spark-data-cleansing-($version}.jar"

logLevel := Level.Warn

mergeStrategy in assembly <<= (mergeStrategy in assembly) { (old) => {
    case PathList("javax", "servlet", xs@_*) => MergeStrategy.last
    case PathList("org", "apache", xs@_*) => MergeStrategy.last
    case PathList("com", "esotericsoftware", xs@_*) => MergeStrategy.last
    case PathList("com", "google", xs@_*) => MergeStrategy.last
    case "about.html" => MergeStrategy.rename
    case x => old(x)
  }
}

libraryDependencies += "org.scala-lang" % "scala-library" % "2.10.5"

libraryDependencies += "org.apache.spark" % "spark-core_2.10" % "1.4.0"

libraryDependencies += "org.apache.spark" % "spark-sql_2.10" % "1.4.0"

libraryDependencies += "org.apache.spark" % "spark-streaming_2.10" % "1.4.0"

libraryDependencies += "org.apache.spark" % "spark-streaming-kafka_2.10" % "1.4.0"

libraryDependencies += "org.apache.httpcomponents" % "httpclient" % "4.5"

libraryDependencies += "org.postgresql" % "postgresql" % "9.4-1201-jdbc41"