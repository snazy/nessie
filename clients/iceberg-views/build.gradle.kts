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
  `java-library`
  jacoco
  `maven-publish`
  signing
  `nessie-conventions`
}

dependencies {
  implementation(nessieProject("nessie-client"))
  implementation(nessieProject("nessie-model"))

  val versionIceberg = dependencyVersion("versionIceberg")
  implementation("org.apache.iceberg:iceberg-api:$versionIceberg")
  implementation("org.apache.iceberg:iceberg-core:$versionIceberg")
  implementation("org.apache.iceberg:iceberg-common:$versionIceberg")
  implementation(
    group = "org.apache.iceberg",
    name = "iceberg-bundled-guava",
    version = versionIceberg,
    configuration = if (isIntegrationsTestingEnabled()) "shadow" else null
  )
  implementation("org.apache.iceberg:iceberg-nessie:$versionIceberg") {
    exclude("org.projectnessie", "*")
  }
  implementation(libs.hadoop.client) {
    exclude("javax.servlet.jsp", "jsp-api")
    exclude("javax.ws.rs", "javax.ws.rs-api")
    exclude("log4j", "log4j")
    exclude("org.slf4j", "slf4j-log4j12")
    exclude("org.slf4j", "slf4j-reload4j")
    exclude("com.sun.jersey", "jersey-servlet")
    exclude("org.apache.hadoop", "hadoop-client")
  }
  compileOnly(libs.microprofile.openapi)

  testImplementation(nessieProject("nessie-versioned-persist-testextension"))
  testImplementation(nessieProject("nessie-versioned-persist-in-memory"))
  testImplementation(nessieProject("nessie-jaxrs-testextension"))
  testImplementation(libs.slf4j.log4j.over.slf4j)
  testCompileOnly(libs.microprofile.openapi)

  testImplementation(platform(libs.junit.bom))
  testImplementation(libs.bundles.junit.testing)
  testRuntimeOnly(libs.junit.jupiter.engine)
}
