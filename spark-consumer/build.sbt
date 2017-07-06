name := "spark-movie"

version := "1.0"

scalaVersion := "2.11.7"

resolvers += "Typesafe Repo" at "http://repo.typesafe.com/typesafe/releases/"

libraryDependencies += "org.apache.spark" %% "spark-core" % "2.1.1"
libraryDependencies += "org.apache.spark" %% "spark-streaming" % "2.1.1"
libraryDependencies += "org.apache.spark" %% "spark-streaming-kafka-0-10" % "2.1.1"

libraryDependencies +=  "com.typesafe.play" %% "play-json" % "2.3.0"

resolvers += "sbt-idea-repo" at "http://mpeltonen.github.com/maven/"
