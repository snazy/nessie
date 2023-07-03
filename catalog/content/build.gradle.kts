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
  id("nessie-conventions-server")
  id("nessie-jacoco")
}

description = "Nessie - REST Catalog - Content related"

extra["maven.name"] = "Nessie - REST Catalog - Content related"

val versionIceberg = libs.versions.iceberg.get()

val sparkScala = useSparkScalaVersionsForProject("3.4", "2.12")

dependencies {
  implementation("org.apache.iceberg:iceberg-core:$versionIceberg")
  implementation(nessieProject(":nessie-model"))
  implementation(nessieProject(":nessie-catalog-api"))

  implementation(libs.hadoop.common) { hadoopExcludes() }

  compileOnly(libs.immutables.builder)
  compileOnly(libs.immutables.value.annotations)
  annotationProcessor(libs.immutables.value.processor)

  implementation(libs.guava)

  implementation(platform(libs.jackson.bom))
  implementation("com.fasterxml.jackson.core:jackson-databind")
  implementation("com.fasterxml.jackson.core:jackson-annotations")

  // javax/jakarta
  compileOnly(libs.jakarta.validation.api)
  compileOnly(libs.javax.validation.api)
  compileOnly(libs.jakarta.annotation.api)
  compileOnly(libs.findbugs.jsr305)

  compileOnly(libs.microprofile.openapi)

  testFixturesApi(libs.guava)

  testFixturesApi("org.apache.iceberg:iceberg-core:$versionIceberg")
  testFixturesApi(libs.hadoop.common) { hadoopExcludes() }

  testFixturesCompileOnly(libs.immutables.builder)
  testFixturesCompileOnly(libs.immutables.value.annotations)
  testFixturesAnnotationProcessor(libs.immutables.value.processor)

  testFixturesImplementation(libs.microprofile.openapi)

  testFixturesApi(platform(libs.junit.bom))
  testFixturesApi(libs.bundles.junit.testing)

  testFixturesImplementation(libs.slf4j.api)
  testFixturesRuntimeOnly(libs.logback.classic)

  testCompileOnly(libs.microprofile.openapi)

  intTestImplementation(
    nessieProject(
      ":nessie-spark-extensions-${sparkScala.sparkMajorVersion}_${sparkScala.scalaMajorVersion}"
    )
  )
  intTestImplementation("org.apache.iceberg:iceberg-nessie:$versionIceberg")
  intTestImplementation(
    "org.apache.iceberg:iceberg-spark-${sparkScala.sparkMajorVersion}_${sparkScala.scalaMajorVersion}:$versionIceberg"
  )
  intTestImplementation(
    "org.apache.iceberg:iceberg-spark-extensions-${sparkScala.sparkMajorVersion}_${sparkScala.scalaMajorVersion}:$versionIceberg"
  )
  intTestImplementation("org.apache.iceberg:iceberg-hive-metastore:$versionIceberg")

  intTestRuntimeOnly(libs.logback.classic)
  intTestImplementation(libs.slf4j.log4j.over.slf4j)
  intTestImplementation("org.apache.spark:spark-sql_${sparkScala.scalaMajorVersion}") {
    forSpark(sparkScala.sparkVersion)
  }
  intTestImplementation("org.apache.spark:spark-core_${sparkScala.scalaMajorVersion}") {
    forSpark(sparkScala.sparkVersion)
  }
  intTestImplementation("org.apache.spark:spark-hive_${sparkScala.scalaMajorVersion}") {
    forSpark(sparkScala.sparkVersion)
  }
}

fun ModuleDependency.hadoopExcludes() {
  exclude("ch.qos.reload4j", "reload4j")
  exclude("com.sun.jersey")
  exclude("commons-cli", "commons-cli")
  exclude("jakarta.activation", "jakarta.activation-api")
  exclude("javax.servlet", "javax.servlet-api")
  exclude("javax.servlet.jsp", "jsp-api")
  exclude("javax.ws.rs", "javax.ws.rs-api")
  exclude("log4j", "log4j")
  exclude("org.slf4j", "slf4j-log4j12")
  exclude("org.slf4j", "slf4j-reload4j")
  exclude("org.eclipse.jetty")
  exclude("org.apache.zookeeper")
}

forceJavaVersionForTestTask("intTest", sparkScala.runtimeJavaVersion)
