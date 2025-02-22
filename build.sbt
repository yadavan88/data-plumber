ThisBuild / organization := "com.yadavan88"
ThisBuild / scalaVersion := "3.3.5"
ThisBuild / name := "data-plumber"

libraryDependencies ++= Seq(
  "org.mongodb" % "mongodb-driver-sync" % "5.3.1",
  "org.postgresql" % "postgresql" % "42.7.5",
  "org.tpolecat" %% "skunk-core" % "1.1.0-M3",
  "org.tpolecat" %% "doobie-core" % "1.0.0-RC7",
  "org.tpolecat" %% "doobie-postgres" % "1.0.0-RC7"
)