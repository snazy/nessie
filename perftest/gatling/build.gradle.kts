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
  jacoco
  `maven-publish`
  scala
  `nessie-conventions`
}

extra["maven.name"] = "Nessie - Perf Test - Gatling"

val scalaVersion = dependencyVersion("versionScala2_13")

dependencies {
  // picks the right dependencies for scala compilation
  forScala(scalaVersion)

  implementation(platform(rootProject))
  implementation(projects.model)
  implementation(projects.clients.client)
  implementation("io.gatling.highcharts:gatling-charts-highcharts") {
    exclude("io.netty", "netty-tcnative-boringssl-static")
    exclude("commons-logging", "commons-logging")
  }
}
