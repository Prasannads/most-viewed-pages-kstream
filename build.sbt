import sbt.{Credentials, Path}

name := "most-viewed-pages-kstream"
organization := "com.joyn.data"
version := "1.0"
scalaVersion := "2.12.10"
credentials += Credentials(Path.userHome / ".sbt" / ".credentials")
resolvers += "io.confluent" at "https://packages.confluent.io/maven/"
resolvers += Resolver.mavenLocal
inThisBuild(List(assemblyJarName in assembly := "most-viewed-pages-kstream.jar"))
libraryDependencies ++= {
  sys.props += "packaging.type" -> "jar"
  Seq(
    "com.github.pureconfig" %% "pureconfig"              % "0.12.2",
    "io.confluent"          % "kafka-avro-serializer"    % "5.3.1",
    "io.confluent"          % "kafka-streams-avro-serde" % "5.3.1",
    "org.apache.kafka"      % "kafka-clients"            % "2.3.1",
    "org.apache.kafka"      % "kafka-streams"            % "2.3.1",
    "org.apache.kafka"      %% "kafka-streams-scala"     % "2.3.1",
    // Logging
    "log4j"                      % "log4j"           % "1.2.17",
    "com.typesafe.akka"          %% "akka-slf4j"     % "2.5.27",
    "com.typesafe.scala-logging" %% "scala-logging"  % "3.9.0",
    "ch.qos.logback"             % "logback-classic" % "1.2.3",
    // Testing
    "org.scalatest"    %% "scalatest"               % "3.1.0",
    "org.apache.kafka" % "kafka-streams-test-utils" % "5.5.0-ccs"
  )
}

AvroConfig / javaSource := baseDirectory.value / "src/main/java/"

fork in run := true
assemblyMergeStrategy in assembly := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case "reference.conf"              => MergeStrategy.concat
  case x                             => MergeStrategy.first
}
