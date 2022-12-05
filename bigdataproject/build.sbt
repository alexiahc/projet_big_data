val scala2Version = "2.13.10"

lazy val root = project
  .in(file("."))
  .settings(
    organization := "upm.bd",
    name := "BigDataProject",
    version := "1.0.0",

    scalaVersion := scala2Version,

    libraryDependencies += "org.scalameta" %% "munit" % "0.7.29" % Test,
    libraryDependencies += "org.apache.spark" %% "spark-core" % "3.3.0"
  )
