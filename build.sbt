name := "funcytown"

version := "0.0.1"

organization := "com.twitter"

scalaVersion := "2.8.1"

resolvers += "Concurrent Maven Repo" at "http://conjars.org/repo"

libraryDependencies += "org.scala-tools.testing" % "specs_2.8.1" % "1.6.6" % "test"

libraryDependencies += "com.twitter" % "kryo" % "2.04"

libraryDependencies += "org.apache.directory.studio" % "org.apache.commons.collections" % "3.2.1"

parallelExecution in Test := false
