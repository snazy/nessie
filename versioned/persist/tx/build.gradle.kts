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

extra["maven.name"] = "Nessie - Versioned - Persist - Transactional"

dependencies {
  implementation(project(":nessie-versioned-persist-adapter"))
  implementation(project(":nessie-versioned-persist-serialize"))
  implementation(project(":nessie-versioned-spi"))
  implementation(libs.guava)
  implementation(libs.findbugs.jsr305)
  compileOnly(libs.immutables.value.annotations)
  annotationProcessor(libs.immutables.value.processor)
  implementation(libs.slf4j.api)

  compileOnly(libs.agroal.pool)
  compileOnly(libs.h2)
  compileOnly(libs.postgresql)

  testImplementation(project(":nessie-versioned-tests"))
  testCompileOnly(libs.immutables.value.annotations)
  testAnnotationProcessor(libs.immutables.value.processor)
  testImplementation(project(":nessie-versioned-persist-testextension"))
  testImplementation(project(":nessie-versioned-persist-tests"))
  testImplementation(project(":nessie-versioned-persist-transactional-test"))
  testRuntimeOnly(libs.h2)
  testRuntimeOnly(libs.postgresql)

  testImplementation(platform(libs.junit.bom))
  testImplementation(libs.bundles.junit.testing)
  testRuntimeOnly(libs.junit.jupiter.engine)
}

tasks.named<Test>("test") { maxParallelForks = Runtime.getRuntime().availableProcessors() }

tasks.named<Test>("intTest") {
  systemProperty("it.nessie.dbs", System.getProperty("it.nessie.dbs", "postgres"))
}
