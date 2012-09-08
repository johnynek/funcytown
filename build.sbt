name := "funcytown"

version := "0.0.1"

organization := "org.bykn"

scalaVersion := "2.9.2"

resolvers += "Concurrent Maven Repo" at "http://conjars.org/repo"

libraryDependencies += "org.scala-tools.testing" % "specs_2.9.1" % "1.6.9" % "test"

libraryDependencies += "com.esotericsoftware.kryo" % "kryo" % "2.16"

parallelExecution in Test := false
