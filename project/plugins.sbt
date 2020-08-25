import sbt.Resolver

addSbtPlugin("com.eed3si9n" % "sbt-assembly" % "0.14.6")
resolvers += Resolver.bintrayIvyRepo("kamon-io", "sbt-plugins")
addSbtPlugin("io.kamon" % "sbt-aspectj-runner" % "1.1.1")
addSbtPlugin("com.cavorite" % "sbt-avro-1-9" % "1.1.7")