import sbtassembly.MergeStrategy

ThisBuild / scalaVersion := "2.13.14"
ThisBuild / organization := "com.shippulse"

lazy val root = (project in file("."))
  .settings(
    name := "ship-pulse",
    libraryDependencies ++= Seq(
      "org.apache.kafka"             %  "kafka-clients"          % "3.6.1",
      "com.fasterxml.jackson.module" %% "jackson-module-scala"   % "2.17.0",
      "ch.qos.logback"               %  "logback-classic"        % "1.5.6",
      "org.postgresql"               %  "postgresql"             % "42.7.4"
    ),
    assembly / assemblyMergeStrategy := {
  case PathList("META-INF", "services", _*) => MergeStrategy.concat
  case PathList("META-INF", xs @ _*)        => MergeStrategy.discard
  case "module-info.class"                  => MergeStrategy.discard
  case _                                    => MergeStrategy.first
}
  )