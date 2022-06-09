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
  `nessie-conventions`
}

extra["maven.artifactId"] = "nessie-jaxrs-testextension"

extra["maven.name"] = "Nessie - JUnit Jupyter Test Extension"

description = "JUnit Jupyter Extension to run tests against an \"embedded\" Nessie instance."

dependencies {
  api(platform(rootProject))
  api(projects.servers.jaxRs)
  api(platform("org.junit:junit-bom"))
  api("org.junit.jupiter:junit-jupiter-api")
  api("org.slf4j:slf4j-api")
  api("org.assertj:assertj-core")

  compileOnly("org.eclipse.microprofile.openapi:microprofile-openapi-api")

  testImplementation(platform(rootProject))
  testCompileOnly("org.eclipse.microprofile.openapi:microprofile-openapi-api")

  testImplementation("org.assertj:assertj-core")
  testImplementation(platform("org.junit:junit-bom"))
  testImplementation("org.junit.jupiter:junit-jupiter-api")
  testImplementation("org.junit.jupiter:junit-jupiter-params")
  testRuntimeOnly("org.junit.jupiter:junit-jupiter-engine")
}
