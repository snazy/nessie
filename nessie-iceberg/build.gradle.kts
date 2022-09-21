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
  id("org.projectnessie.buildsupport.ide-integration")
  `nessie-conventions`
}

// To fix circular dependencies with NessieClient, certain projects need to use the same Nessie
// version as Iceberg/Delta has.
// Allow overriding the Iceberg version used by Nessie and the Nessie version used by integration
// tests that depend on Iceberg.
val versionIceberg: String =
  System.getProperty("nessie.versionIceberg", libs.versions.iceberg.get())
val versionClientNessie: String =
  System.getProperty("nessie.versionClientNessie", libs.versions.nessieClientVersion.get())

mapOf(
    "versionCheckstyle" to libs.versions.checkstyle.get(),
    "versionClientNessie" to versionClientNessie,
    "versionIceberg" to versionIceberg,
    "versionErrorProneCore" to libs.versions.errorprone.get(),
    "versionErrorProneSlf4j" to libs.versions.errorproneSlf4j.get(),
    "versionGoogleJavaFormat" to libs.versions.googleJavaFormat.get(),
    "versionJacoco" to libs.versions.jacoco.get(),
    "versionJandex" to libs.versions.jandex.get()
  )
  .plus(loadProperties(file("../clients/spark-scala.properties")))
  .forEach { (k, v) -> extra[k.toString()] = v }

publishingHelper {
  nessieRepoName.set("nessie")
  inceptionYear.set("2020")
}
