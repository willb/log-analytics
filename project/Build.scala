import sbt._
import Keys._

object LABuild  extends Build {
  val VERSION = "0.0.1-SNAPSHOT"
  
  lazy val common = project settings(commonSettings : _*)
  
  lazy val analysis = project settings(analysisSettings : _*) dependsOn(common)
  
  lazy val repl = project settings(replSettings : _*) dependsOn(analysis)
  
  lazy val root = (project in file(".")).aggregate(common, analysis)
  
  def baseSettings = Defaults.defaultSettings ++ Seq(
    organization := "com.redhat.et",
    version := VERSION,
    scalaVersion := SCALA_VERSION,
    resolvers ++= Seq(
      "Sonatype Snapshots" at "https://oss.sonatype.org/content/repositories/snapshots/",
      "Sonatype Releases" at "https://oss.sonatype.org/content/repositories/releases/",
      "Akka Repo" at "http://repo.akka.io/repository",
      "Will's bintray" at "https://dl.bintray.com/willb/maven/",
      "spray" at "http://repo.spray.io/"
    ),
    libraryDependencies ++= Seq(
        "org.json4s" %%  "json4s-jackson" % "3.2.10",
        "org.json4s" %% "json4s-ext" % "3.2.11",
        "joda-time" % "joda-time" % "2.7",
        "com.redhat.et" %% "silex" % "0.0.9"
    ),
    scalacOptions ++= Seq("-feature", "-Yrepl-sync", "-target:jvm-1.7", "-Xlint")
  )
  
  def sparkSettings = Seq(
    libraryDependencies ++= Seq(
        "org.apache.spark" %% "spark-core" % SPARK_VERSION,
        "org.apache.spark" %% "spark-sql" % SPARK_VERSION,
        "org.apache.spark" %% "spark-catalyst" % SPARK_VERSION,
        "org.apache.spark" %% "spark-hive" % SPARK_VERSION,
        "org.apache.spark" %% "spark-mllib" % SPARK_VERSION,
        "org.scala-lang" % "scala-reflect" % SCALA_VERSION,
        "org.elasticsearch" %% "elasticsearch-spark" % "2.1.0.rc1"
    )
  )
  
  def breezeSettings = Seq(
    libraryDependencies ++= Seq(
      "org.scalanlp" %% "breeze" % "0.12"
    )
  )
  
  def testSettings = Seq(
    fork := true,
    libraryDependencies ++= Seq(
      "org.scalacheck" %% "scalacheck" % "1.11.3" % "test",
      "org.scalatest" %% "scalatest" % "2.2.4" % "test"
    )
  )
  
  def jsonSettings = Seq(
    libraryDependencies ++= Seq(
      "org.json4s" %% "json4s-jackson" % "3.2.10",
      "org.json4s" %% "json4s-ext" % "3.2.11",
      "joda-time" % "joda-time" % "2.7"
    ) 
  )
  
  def dispatchSettings = Seq(
    libraryDependencies += 
      "net.databinder.dispatch" %% "dispatch-core" % "0.11.1"
  )
  
  def commonSettings = baseSettings ++ sparkSettings ++ jsonSettings ++ Seq(
    name := "log-analysis-common"
  )
  
  def analysisSettings = commonSettings ++ breezeSettings ++ testSettings ++ Seq(
    name := "log-analysis",
    initialCommands in console :=
      """
        |import org.apache.spark.SparkConf
        |import org.apache.spark.SparkContext
        |import org.apache.spark.SparkContext._
        |import org.apache.spark.rdd.RDD
        |val app = com.redhat.et.silex.app.ReplApp.makeApp
        |val spark = app.context
        |val sqlc = app.sqlContext
        |import sqlc._
        |
      """.stripMargin,
    cleanupCommands in console := "spark.stop"
  )
  
  def replSettings = analysisSettings ++ Seq(
    name := "log-analysis-repl",
    libraryDependencies += "org.scala-lang" % "jline" % SCALA_VERSION
  )
  
  val SPARK_VERSION = "1.6.1"
  val SCALA_VERSION = "2.11.8"
}
