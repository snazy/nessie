/*
 * Copyright (C) 2025 Dremio
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

package shadow

import org.gradle.api.DefaultTask
import org.gradle.api.file.ConfigurableFileCollection
import org.gradle.api.tasks.CacheableTask
import org.gradle.api.tasks.Classpath
import org.gradle.api.tasks.InputFiles
import org.gradle.api.tasks.TaskAction
import org.gradle.api.tasks.util.PatternSet

/**
 * Helper task to temporarily add to your build script to find resources in the classpath that were
 * identified as duplicates by [MergePropertiesResourceTransformer] or
 * [DeduplicatingResourceTransformer].
 *
 * First, add the task to your build script:
 * ```kotlin
 * val findResources by tasks.registering(FindResourceInClasspath::class) {
 *   // add configurations to search for resources in dependency jars
 *   classpath.from(configurations.runtimeClasspath)
 *   // the patterns to search for (it is a Gradle PatternFilterable)
 *   include(
 *     "META-INF/...",
 *   )
 * }
 * ```
 *
 * Then let `shadowJar` depend on the task, or just run the above task manually.
 *
 * ```kotlin
 * tasks.named("shadowJar") {
 *   dependsOn(findResources)
 * }
 * ```
 */
@Suppress("unused")
@CacheableTask
abstract class FindResourceInClasspath(private val patternSet: PatternSet = PatternSet()) :
  DefaultTask() {
  @get:InputFiles @get:Classpath abstract val classpath: ConfigurableFileCollection

  @TaskAction
  fun findResources() {
    classpath.forEach { file ->
      logger.lifecycle("scanning {}", file)

      project.zipTree(file).matching(patternSet).forEach { entry ->
        logger.lifecycle("  -> {}", entry)
      }
    }
  }
}
