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
package org.projectnessie.catalog.content.merge;

import static com.google.common.base.Preconditions.checkState;

import java.util.function.BiConsumer;
import javax.annotation.Nullable;
import org.apache.iceberg.io.FileIO;
import org.immutables.value.Value;
import org.projectnessie.catalog.content.iceberg.metadata.MetadataIO;
import org.projectnessie.model.Content;
import org.projectnessie.model.ContentKey;

@Value.Immutable
public interface ContentMerge<C extends Content> {
  ContentKey sourceKey();

  Content.Type contentType();

  C mergeBaseTable();

  @Nullable
  @jakarta.annotation.Nullable
  C sourceTable();

  @Nullable
  @jakarta.annotation.Nullable
  C targetTable();

  MetadataIO metadataIO();

  FileIO fileIO();

  @Value.Check
  default void verifyContentType() {
    BiConsumer<C, String> verifySingle =
        (c, attr) -> {
          if (c != null) {
            checkState(
                c.getType().equals(contentType()),
                "Expecting content type %s, but got %s in %s",
                contentType(),
                c.getType(),
                attr);
          }
        };

    verifySingle.accept(mergeBaseTable(), "merge-base");
    verifySingle.accept(sourceTable(), "source");
    verifySingle.accept(targetTable(), "target");
  }
}
