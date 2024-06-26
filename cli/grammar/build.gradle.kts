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
  alias(libs.plugins.jmh)
}

val congocc by configurations.creating
val syntaxGen by configurations.creating

configurations.compileOnly { extendsFrom(syntaxGen) }

configurations.testFixturesApi { extendsFrom(syntaxGen) }

dependencies {
  compileOnly(libs.jakarta.annotation.api)
  compileOnly(libs.immutables.value.annotations)
  annotationProcessor(libs.immutables.value.processor)

  implementation(libs.guava)
  implementation(libs.slf4j.api)

  syntaxGen(libs.congocc)
  syntaxGen(libs.jline)

  congocc(libs.congocc)

  testFixturesApi(platform(libs.junit.bom))
  testFixturesApi(libs.bundles.junit.testing)
  testFixturesApi("org.antlr:antlr4:${libs.antlr.antlr4.runtime.get().version}")
  testFixturesApi(libs.jakarta.annotation.api)
  testFixturesApi(libs.immutables.value.annotations)

  testFixturesRuntimeOnly(libs.logback.classic)

  jmhImplementation(libs.jmh.core)
  jmhAnnotationProcessor(libs.jmh.generator.annprocess)
}

abstract class Generate : JavaExec() {
  @get:InputDirectory
  @get:PathSensitive(PathSensitivity.RELATIVE)
  abstract val sourceDir: DirectoryProperty

  @get:OutputDirectory abstract val outputDir: DirectoryProperty
}

val genNessieGrammarDir = project.layout.buildDirectory.dir("generated/sources/congocc/nessie")
val genJsonGrammarDir = project.layout.buildDirectory.dir("generated/sources/congocc/json")
val genNessieSyntaxDir = project.layout.buildDirectory.dir("generated/resources/nessie-syntax")

val prepGenerateNessieCcc by tasks.registering(Delete::class) { delete(genNessieGrammarDir) }
val prepGenerateJsonCcc by tasks.registering(Delete::class) { delete(genJsonGrammarDir) }
val prepGenerateNessieSyntax by tasks.registering(Delete::class) { delete(genNessieGrammarDir) }

val generateNessieCcc by
  tasks.registering(Generate::class) {
    dependsOn(prepGenerateNessieCcc)

    sourceDir = projectDir.resolve("src/main/congocc/nessie")
    val sourceFile =
      sourceDir.get().file("nessie-cli-java.ccc").asFile.relativeTo(projectDir).toString()
    outputDir = genNessieGrammarDir

    classpath(congocc)

    mainClass = "org.congocc.app.Main"
    workingDir(projectDir)

    // TODO the following does NOT work with Gradle's configuration cache
    //    argumentProviders.add(
    //      CommandLineArgumentProvider {
    //        val base =
    //          listOf("-d", genNessieGrammarDir.get().asFile.toString(), "-jdk17", "-n",
    // sourceFile)
    //        if (logger.isInfoEnabled) base else (base + listOf("-q"))
    //      }
    //    )
    // TODO this workaround uses _absolute_ paths, which should really not be used with the build
    //  cache
    args("-d", genNessieGrammarDir.get().asFile, "-jdk17", "-n", sourceFile)
    if (!logger.isInfoEnabled) {
      args("-q")
    }
  }

val generateJsonCcc by
  tasks.registering(Generate::class) {
    dependsOn(prepGenerateJsonCcc)

    sourceDir = projectDir.resolve("src/main/congocc/json")
    val sourceFile = sourceDir.get().file("jsonc.ccc").asFile.relativeTo(projectDir)
    outputDir = genJsonGrammarDir

    classpath(congocc)

    mainClass = "org.congocc.app.Main"
    workingDir(projectDir)

    // TODO the following does NOT work with Gradle's configuration cache
    //    argumentProviders.add(
    //      CommandLineArgumentProvider {
    //        val base =
    //          listOf(
    //            "-d",
    //            genJsonGrammarDir.get().asFile.toString(),
    //            "-jdk17",
    //            "-n",
    //            sourceDir.get().file("jsonc.ccc").asFile.relativeTo(projectDir).toString()
    //          )
    //        if (logger.isInfoEnabled) base else (base + listOf("-q"))
    //      }
    //    )
    // TODO this workaround uses _absolute_ paths, which should really not be used with the build
    //  cache
    args("-d", genJsonGrammarDir.get().asFile, "-jdk17", "-n", sourceFile)
    if (!logger.isInfoEnabled) {
      args("-q")
    }
  }

val compileJava = tasks.named("compileJava") { dependsOn(generateNessieCcc, generateJsonCcc) }

val generateNessieSyntax by
  tasks.registering(Generate::class) {
    dependsOn(compileJava, prepGenerateNessieSyntax)

    sourceDir = projectDir.resolve("src/main/congocc/nessie")
    outputDir = genNessieSyntaxDir

    classpath(syntaxGen, configurations.runtimeClasspath, compileJava)

    mainClass = "org.projectnessie.nessie.cli.syntax.SyntaxTool"
    workingDir(projectDir)
    args(
      genNessieSyntaxDir.get().asFile.relativeTo(projectDir),
      sourceDir.get().file("nessie-cli-java.ccc").asFile.relativeTo(projectDir)
    )
  }

tasks.named("processResources") { dependsOn(generateNessieSyntax) }

tasks.named("sourcesJar") { dependsOn(generateNessieSyntax) }

sourceSets {
  main {
    java {
      srcDir(genNessieGrammarDir)
      srcDir(genJsonGrammarDir)
    }
    resources { srcDir(genNessieSyntaxDir) }
  }
}

tasks.withType<Checkstyle>().configureEach {
  // Cannot exclude build/ as a "general configuration", because the Checstyle task creates an
  // ant script behind the scenes, and that only supports "string" pattern matching using.
  // The base directories are the source directories, so all patterns match against paths
  // relative to a source-directory, not against full path names, not even relative to the current
  // project.
  exclude("org/projectnessie/nessie/cli/grammar/*", "org/projectnessie/nessie/cli/jsongrammar/*")
}

tasks.named("processJmhJandexIndex").configure { enabled = false }

tasks.named("processTestJandexIndex").configure { enabled = false }

jmh { jmhVersion = libs.versions.jmh.get() }

tasks.named<Jar>("jmhJar") { manifest { attributes["Multi-Release"] = "true" } }
