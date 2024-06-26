/*
 * Copyright 2021 Elastacloud Limited
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import sbt.url

lazy val root = (project in file("."))
  .settings(commonSettings)

val sparkVersion = settingKey[String]("Spark version")
val sparkExcelVersion = settingKey[String]("Version of the Spark Excel library")
val scalaTestVersion = settingKey[String]("ScalaTest version")
val poiVersion = settingKey[String]("Apache POI version")
val log4JVersion = settingKey[String]("Apache Log4J version")

name := "spark-excel"
organization := "com.elastacloud"
description := "A custom data reader for Microsoft Excel documents based on the Apache POI project"
homepage := Some(url("https://www.elastacloud.com"))
developers += Developer(id = "dazfuller", name = "Darren Fuller", email = "darren@elastacloud.com", url = url("https://github.com/dazfuller"))
scmInfo := Some(ScmInfo(url("https://github.com/elastacloud/spark-excel"), "git@github.com:elastacloud/spark-excel.git"))
licenses += ("Apache License, Version 2.0", url("https://www.apache.org/licenses/LICENSE-2.0"))

Compile / unmanagedSourceDirectories ++= {
  if (sparkVersion.value < "3.3.0") {
    Seq(baseDirectory.value / "src/main/3.0/scala")
  } else if (sparkVersion.value < "3.4.0") {
    Seq(baseDirectory.value / "src/main/3.3/scala")
  } else {
    Seq(baseDirectory.value / "src/main/3.4/scala")
  }
}

Compile / packageBin / publishArtifact := false
Compile / packageDoc / publishArtifact := false
Compile / packageSrc / publishArtifact := false

artifactName := { (_: ScalaVersion, _: ModuleID, artifact: Artifact) =>
  s"${artifact.name}-${(ThisBuild / version).value}.${artifact.extension}"
}

target := file("target") / s"spark-${sparkVersion.value}"

// Add Spark and POI dependencies
libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion.value % Provided,
  "org.apache.spark" %% "spark-sql" % sparkVersion.value % Provided,
  "org.apache.poi" % "poi" % poiVersion.value % Compile,
  "org.apache.poi" % "poi-ooxml" % poiVersion.value % Compile,
  "org.apache.poi" % "poi-ooxml-lite" % poiVersion.value % Compile,
  "org.apache.commons" % "commons-compress" % "1.21" % Compile,
  "org.apache.commons" % "commons-collections4" % "4.4" % Compile,
  "commons-io" % "commons-io" % "2.11.0" % Compile,
  "org.apache.logging.log4j" % "log4j-core" % log4JVersion.value % Compile,
  "org.apache.logging.log4j" % "log4j-api" % log4JVersion.value % Compile
)

// Setup test dependencies and configuration
Test / parallelExecution := false
Test / fork := true

libraryDependencies ++= Seq(
  "org.scalactic" %% "scalactic" % scalaTestVersion.value,
  "org.scalatest" %% "scalatest" % scalaTestVersion.value % Test
)

coverageOutputCobertura := true
coverageOutputHTML := true
coverageMinimumStmtTotal := 70
coverageFailOnMinimum := false
coverageHighlighting := true

ThisBuild / assemblyShadeRules := Seq(
  ShadeRule.rename("org.apache.poi.**" -> "elastashade.poi.@1").inAll,
  ShadeRule.rename("org.apache.commons.collections4.**" -> "elastashade.commons.collections4.@1").inAll,
  ShadeRule.rename("org.apache.commons.compress.**" -> "elastashade.commons.compress.@1").inAll,
  ShadeRule.rename("org.apache.logging.log4j.**" -> "elastashade.logging.log4j.@1").inAll,
  ShadeRule.rename("org.apache.commons.io.**" -> "elastashade.commons.io.@1").inAll
)

ThisBuild / assemblyMergeStrategy := {
  //case PathList("META-INF", "services", "org.apache.spark.sql.sources.DataSourceRegister") => MergeStrategy.concat
  case PathList("META-INF", "services", _@_*) => MergeStrategy.first
  case PathList("com", "elastacloud", _@_*) => MergeStrategy.last
  case PathList("elastashade", "poi", _@_*) => MergeStrategy.last
  case PathList("elastashade", "commons", "io", _@_*) => MergeStrategy.last
  case PathList("elastashade", "commons", "compress", _@_*) => MergeStrategy.last
  case PathList("elastashade", "commons", "collections4", _@_*) => MergeStrategy.last
  case PathList("elastashade", "logging", "log4j", _@_*) => MergeStrategy.last
  case PathList("org", "apache", "xmlbeans", _@_*) => MergeStrategy.last
  case PathList("org", "openxmlformats", "schemas", _@_*) => MergeStrategy.last
  case PathList("schemaorg_apache_xmlbeans", _@_*) => MergeStrategy.last
  case _ => MergeStrategy.discard
}

assembly / assemblyOption ~= {
  _.withIncludeScala(false)
}

assembly / assemblyJarName := s"${name.value}-${version.value}.jar"

assembly / artifact := {
  val art = (assembly / artifact).value
  art.withClassifier(Some("assembly"))
}

addArtifact(Compile / assembly / artifact, assembly)

// Define common settings for the library
val commonSettings = Seq(
  sparkVersion := System.getProperty("sparkVersion", "3.5.1"),
  sparkExcelVersion := "0.1.12",
  version := s"${sparkVersion.value}_${sparkExcelVersion.value}",
  scalaVersion := {
    if (sparkVersion.value < "3.2.0") {
      "2.12.10"
    } else if (sparkVersion.value < "3.4.0") {
      "2.12.14"    }
    else if (sparkVersion.value < "3.5.0") {
      "2.12.15"
    } else {
      "2.12.18"
    }
  },
  scalaTestVersion := "3.2.16",
  poiVersion := "5.2.5",
  log4JVersion := "2.20.0",
  crossVersion := CrossVersion.disabled
)
