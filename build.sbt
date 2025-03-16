ThisBuild / organization := "com.yadavan88"
ThisBuild / scalaVersion := "3.3.5"
ThisBuild / name := "data-plumber"

libraryDependencies ++= Seq(
  "org.mongodb" % "mongodb-driver-sync" % "5.3.1",
  "org.postgresql" % "postgresql" % "42.7.5",
  "org.tpolecat" %% "doobie-core" % "1.0.0-RC7",
  "org.tpolecat" %% "doobie-postgres" % "1.0.0-RC7",
  "dev.profunktor" %% "redis4cats-effects" % "1.7.2",
  "com.augustnagro" %% "magnum" % "1.3.1",
  "co.fs2" %% "fs2-io" % "3.11.0",
  "co.fs2" %% "fs2-core" % "3.11.0"
)
