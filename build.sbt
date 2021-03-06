scalaVersion := "2.11.8"

scalacOptions ++= Seq("-deprecation")

resolvers += Resolver.sonatypeRepo("releases")

// grading libraries
libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "2.1.0",
  "org.apache.spark" %% "spark-sql" % "2.1.0"
)

libraryDependencies += "mysql" % "mysql-connector-java" % "5.1.44"

libraryDependencies += "junit" % "junit" % "4.10" % "test"

