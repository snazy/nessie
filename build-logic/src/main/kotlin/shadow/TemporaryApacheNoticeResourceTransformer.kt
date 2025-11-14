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

import com.github.jengelman.gradle.plugins.shadow.transformers.ApacheNoticeResourceTransformer
import com.github.jengelman.gradle.plugins.shadow.transformers.CacheableTransformer
import javax.inject.Inject
import org.gradle.api.file.FileTreeElement
import org.gradle.api.model.ObjectFactory

@CacheableTransformer
open class TemporaryApacheNoticeResourceTransformer : ApacheNoticeResourceTransformer {
  @Suppress("unused") @Inject constructor(objectFactory: ObjectFactory) : super(objectFactory)

  private val patterns =
    setOf("NOTICE", "META-INF/NOTICE", "META-INF/NOTICE.txt", "META-INF/NOTICE.md")

  override fun canTransformResource(element: FileTreeElement): Boolean {
    val path = element.path
    return patterns.any { it.equals(path, ignoreCase = true) }
  }
}
