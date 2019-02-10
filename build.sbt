name := "reflection-test"

version := "0.1"
val sparkVersion = "2.4.0"

scalaVersion := "2.11.12"

scalacOptions += "-Ypartial-unification"
libraryDependencies += "org.apache.spark" %% "spark-core" % sparkVersion exclude("org.glassfish.hk2.external", "javax.inject")
libraryDependencies += "org.apache.spark" %% "spark-sql" % sparkVersion exclude("org.glassfish.hk2.external", "javax.inject")
libraryDependencies += "org.scala-lang" % "scala-reflect" % scalaVersion.value
libraryDependencies += "org.typelevel" %% "cats-core" % "1.6.0"
