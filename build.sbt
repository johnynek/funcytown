name := "funcytown"

version := "0.0.1"

organization := "org.bykn"

scalaVersion := "2.9.2"

resolvers ++= Seq(
  // Use ScalaCheck
  "snapshots" at "http://oss.sonatype.org/content/repositories/snapshots",
  "releases"  at "http://oss.sonatype.org/content/repositories/releases",
  "Concurrent Maven Repo" at "http://conjars.org/repo"
)

libraryDependencies ++= Seq(
  "org.scalacheck" %% "scalacheck" % "1.10.0" % "test",
  "org.scala-tools.testing" % "specs_2.9.0-1" % "1.6.8" % "test",
  "com.esotericsoftware.kryo" % "kryo" % "2.16"
)

parallelExecution in Test := false
