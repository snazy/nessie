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
  id("com.diffplug.spotless") version "6.7.0"
  id("com.github.johnrengelman.plugin-shadow") version "7.1.2"
  id("com.github.node-gradle.node") version "3.3.0"
  id("com.github.vlsi.jandex") version "1.80"
  id("io.gatling.gradle") version "3.7.6.3"
  id("io.github.gradle-nexus.publish-plugin") version "1.1.0"
  id("io.quarkus") version "2.9.2.Final"
  id("me.champeau.jmh") version "0.6.6"
  id("net.ltgt.errorprone") version "2.0.2"
  id("org.jetbrains.gradle.plugin.idea-ext") version "1.1.4"
  id("org.projectnessie") version "0.27.3"
  val nessieBuildPlugins = "0.1.5"
  id("org.projectnessie.buildsupport.attach-test-jar") version nessieBuildPlugins
  id("org.projectnessie.buildsupport.checkstyle") version nessieBuildPlugins
  id("org.projectnessie.buildsupport.errorprone") version nessieBuildPlugins
  id("org.projectnessie.buildsupport.ide-integration") version nessieBuildPlugins
  id("org.projectnessie.buildsupport.jacoco") version nessieBuildPlugins
  id("org.projectnessie.buildsupport.jacoco-aggregator") version nessieBuildPlugins
  id("org.projectnessie.buildsupport.jandex") version nessieBuildPlugins
  id("org.projectnessie.buildsupport.protobuf") version nessieBuildPlugins
  id("org.projectnessie.buildsupport.publishing") version nessieBuildPlugins
  id("org.projectnessie.buildsupport.reflectionconfig") version nessieBuildPlugins
  id("org.projectnessie.buildsupport.spotless") version nessieBuildPlugins
  id("org.projectnessie.smallrye-open-api") version nessieBuildPlugins
}
