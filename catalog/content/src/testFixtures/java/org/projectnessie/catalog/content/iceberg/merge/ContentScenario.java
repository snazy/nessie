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
package org.projectnessie.catalog.content.iceberg.merge;

import com.google.errorprone.annotations.CanIgnoreReturnValue;
import java.util.List;
import javax.annotation.Nullable;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.io.FileIO;
import org.immutables.value.Value;
import org.projectnessie.catalog.content.iceberg.LocalFileIO;
import org.projectnessie.catalog.content.iceberg.ops.CatalogTableOperations;

@Value.Immutable
public interface ContentScenario {
  static Builder builder() {
    return ImmutableContentScenario.builder();
  }

  List<DataAction> actions();

  @Value.Default
  default String namespaceName() {
    return "namespace_name";
  }

  @Value.Default
  default String tableName() {
    return "table_name";
  }

  @Value.Derived
  default Namespace namespace() {
    return Namespace.of(namespaceName());
  }

  @Value.Derived
  default TableIdentifier tableIdentifier() {
    return TableIdentifier.of(namespace(), tableName());
  }

  @Value.Default
  default FileIO fileIO() {
    return new LocalFileIO();
  }

  @Value.Lazy
  default CatalogTableOperations ops() {
    return new CatalogTableOperations(fileIO(), tableName());
  }

  interface Builder {
    @CanIgnoreReturnValue
    Builder namespaceName(String namespaceName);

    @CanIgnoreReturnValue
    Builder tableName(String tableName);

    @CanIgnoreReturnValue
    Builder addActions(DataAction action);

    @CanIgnoreReturnValue
    Builder addActions(DataAction... actions);

    @CanIgnoreReturnValue
    Builder fileIO(FileIO fileIO);

    ContentScenario build();
  }

  interface DataAction {
    String id();
  }

  @Value.Immutable
  interface ShowChanges extends DataAction {
    static ShowChanges showChanges(String mergeBase, String source, String target) {
      return ImmutableShowChanges.of(mergeBase, source, target);
    }

    @Value.Redacted
    @Override
    default String id() {
      return null;
    }

    @Value.Parameter(order = 1)
    String mergeBase();

    @Value.Parameter(order = 2)
    String source();

    @Value.Parameter(order = 3)
    String target();
  }

  @Value.Immutable
  interface SQL extends DataAction {
    static SQL sql(String sql) {
      return sql(null, sql);
    }

    static SQL sql(String id, String sql) {
      return ImmutableSQL.of(id, sql);
    }

    @Value.Parameter(order = 1)
    @Nullable
    @Override
    String id();

    @Value.Parameter(order = 2)
    String sql();
  }
}
