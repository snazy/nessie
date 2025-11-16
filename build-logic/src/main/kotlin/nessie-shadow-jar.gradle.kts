/*
 * Copyright (C) 2023 Dremio
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

import com.github.jengelman.gradle.plugins.shadow.tasks.ShadowJar
import com.github.jengelman.gradle.plugins.shadow.transformers.AppendingTransformer
import kotlin.jvm.java
import shadow.MergeLicenseResourceTransformer
import shadow.TemporaryApacheNoticeResourceTransformer

plugins { id("com.gradleup.shadow") }

val shadowJar = tasks.named<ShadowJar>("shadowJar")

shadowJar.configure {
  val c = ShadowJar::class.java
  val classResource = c.name.replace('.', '/') + ".class"
  val containingJarUri = c.classLoader.getResource(classResource)!!.toURI().schemeSpecificPart
  if (!containingJarUri.contains("shadow-gradle-plugin-9.2.2.jar")) {
    throw GradleException(
      "Check if https://github.com/GradleUp/shadow/pull/1843 has been merged and if so, replace TemporaryApacheLicenseResourceTransformer with ApacheLicenseResourceTransformer"
    )
  }

  outputs.cacheIf { false } // do not cache uber/shaded jars
  archiveClassifier = ""
  mergeServiceFiles()
}

tasks.named<Jar>("jar").configure {
  dependsOn(shadowJar)
  archiveClassifier = "raw"
}

tasks.withType<ShadowJar>().configureEach {
  exclude("META-INF/jandex.idx")
  isZip64 = true
  duplicatesStrategy = DuplicatesStrategy.INCLUDE
  failOnDuplicateEntries = false
  transform(MergeLicenseResourceTransformer::class.java) {
    artifactLicense.value { project.rootProject.file("gradle/license/Apache-2.0-License") }
  }
  transform(TemporaryApacheNoticeResourceTransformer::class.java)
  transform(AppendingTransformer::class.java) { resource = "META-INF/DEPENDENCIES" }
}
