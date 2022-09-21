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
  `maven-publish`
  signing
  `nessie-conventions`
}

extra["maven.name"] = "Nessie - REST-API Tests"

description = "Artifact for REST-API tests, includes Glassfish/Jersey/Weld implementation."

dependencies {
  implementation(project(":nessie-client"))
  implementation(libs.guava)
  api(libs.rest.assured)
  implementation(libs.findbugs.jsr305)

  api(libs.assertj.core)
  api(platform(libs.junit.bom))
  api(libs.junit.jupiter.api)
  api(libs.junit.jupiter.params)

  compileOnly(libs.microprofile.openapi)
  compileOnly(libs.jakarta.validation.api)

  implementation(platform(libs.jackson.bom))
  implementation(libs.jackson.databind)
  compileOnly(libs.jackson.annotations)

  testImplementation(project(":nessie-jaxrs-testextension"))
  testImplementation(libs.slf4j.jcl.over.slf4j)
  testRuntimeOnly(libs.h2)

  testCompileOnly(libs.microprofile.openapi)

  testImplementation(libs.bundles.junit.testing)
  testRuntimeOnly(libs.junit.jupiter.engine)
}

tasks.named<Test>("test") { maxParallelForks = Runtime.getRuntime().availableProcessors() }
