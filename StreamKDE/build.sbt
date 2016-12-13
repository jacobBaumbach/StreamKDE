name := "StreamKDE"

version := "1.0"

scalaVersion := "2.11.8"

resolvers += "Sonatype Releases" at "https://oss.sonatype.org/content/repositories/releases/"
resolvers += "Sonatype OSS Snapshots" at "https://oss.sonatype.org/content/repositories/snapshots"

libraryDependencies ++= Seq(
  "co.theasi" %% "plotly" % "0.2-SNAPSHOT",
  "com.twitter" %% "algebird-core" % "0.12.3",
  "com.twitter" %% "algebird-util" % "0.12.3",
  "org.scalanlp" %% "breeze" % "0.12",
  "org.scalanlp" %% "breeze-natives" % "0.12",
  "org.scalactic" %% "scalactic" % "3.0.1",
  "org.scalatest" %% "scalatest" % "3.0.1" % "test")
    