import AssemblyKeys._

name := "funcytown"

version := "0.0.1"

organization := "com.twitter"

scalaVersion := "2.8.1"

resolvers += "Concurrent Maven Repo" at "http://conjars.org/repo"

libraryDependencies += "org.scala-tools.testing" % "specs_2.8.1" % "1.6.6" % "test"

libraryDependencies += "com.twitter" % "kryo" % "2.04"

parallelExecution in Test := false

seq(assemblySettings: _*)

// Janino includes a broken signature, and is not needed:
// excludedJars in assembly <<= (fullClasspath in assembly) map { cp =>
//  cp filter {_.data.getName == "janino-2.5.16.jar"}
//}
