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

extra["maven.name"] = "Nessie - GC - Base Implementation"

description =
  "Mark and sweep GC base functionality to identify live contents, map to live files, list existing files and to purge orphan files."

dependencies {
  compileOnly(libs.errorprone.annotations)
  compileOnly(libs.immutables.value.annotations)
  annotationProcessor(libs.immutables.value.processor)

  implementation(project(":nessie-client"))
  implementation(libs.slf4j.api)
  implementation(libs.guava)
  implementation(libs.agrona)

  compileOnly(libs.microprofile.openapi)
  compileOnly(libs.jakarta.validation.api)
  compileOnly(libs.findbugs.jsr305)

  compileOnly(platform(libs.jackson.bom))
  compileOnly(libs.jackson.annotations)

  testImplementation(project(":nessie-gc-base-tests"))
  testImplementation(project(":nessie-jaxrs-testextension"))

  testRuntimeOnly(libs.logback.classic)

  testCompileOnly(libs.microprofile.openapi)
  testCompileOnly(libs.jakarta.validation.api)
  testCompileOnly(libs.immutables.value.annotations)
  testAnnotationProcessor(libs.immutables.value.processor)

  testCompileOnly(platform(libs.jackson.bom))
  testCompileOnly(libs.jackson.annotations)

  testImplementation(platform(libs.junit.bom))
  testImplementation(libs.bundles.junit.testing)
  testRuntimeOnly(libs.junit.jupiter.engine)
}
