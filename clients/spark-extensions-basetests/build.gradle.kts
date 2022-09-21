/*
 * Copyright (C) 2022 Dremio
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

plugins {
  `maven-publish`
  signing
  scala
  `nessie-conventions`
}

val sparkScala = getSparkScalaVersionsForProject()

dependencies {
  // picks the right dependencies for scala compilation
  forScala(sparkScala.scalaVersion)

  implementation(nessieProject("nessie-spark-extensions-grammar"))
  compileOnly("org.apache.spark:spark-hive_${sparkScala.scalaMajorVersion}") {
    forSpark(sparkScala.sparkVersion)
  }
  implementation("org.apache.spark:spark-sql_${sparkScala.scalaMajorVersion}") {
    forSpark(sparkScala.sparkVersion)
  }

  implementation(nessieClientForIceberg())

  implementation(libs.microprofile.openapi)

  compileOnly(libs.errorprone.annotations)
  compileOnly(libs.immutables.value.annotations)
  annotationProcessor(libs.immutables.value.processor)

  implementation(platform(libs.junit.bom))
  implementation(libs.bundles.junit.testing)
}
