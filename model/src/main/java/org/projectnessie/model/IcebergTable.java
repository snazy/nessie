/*
 * Copyright (C) 2020 Dremio
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
package org.projectnessie.model;

import com.fasterxml.jackson.annotation.JsonTypeName;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import javax.validation.constraints.NotNull;
import org.eclipse.microprofile.openapi.annotations.enums.SchemaType;
import org.eclipse.microprofile.openapi.annotations.media.Schema;
import org.immutables.value.Value;

/**
 * Represents the global state of an Iceberg table in Nessie. An Iceberg table is globally
 * identified via its fully qualified name via {@link ContentsKey} plus a unique ID, the latter is
 * represented via {@link Contents#getId()}.
 *
 * <p>A Nessie commit-operation, performed via {@link
 * org.projectnessie.api.TreeApi#commitMultipleOperations(String, String, Operations)}, for Iceberg
 * consists of a {@link Operation.Put} with an {@link IcebergSnapshot} <em>and</em> a {@link
 * Operation.PutGlobal} with an {@link IcebergTable}.
 */
@Schema(
    type = SchemaType.OBJECT,
    title = "Iceberg table global state",
    description =
        "Represents the global state of an Iceberg table in Nessie. An Iceberg table is globally"
            + " identified via its fully qualified name via 'ContentsKey' plus a unique ID, the latter is "
            + " represented via 'Contents.id'.\n"
            + "\n"
            + "A Nessie commit-operation, performed via 'TreeApi.commitMultipleOperations', for Iceberg "
            + "consists of a 'Operation.Put' with an 'IcebergSnapshot' and a "
            + "'Operation.PutGlobal' with an 'IcebergTable'.\n"
            + "\n"
            + "During a commit-operation, Nessie checks whether the known global state of the "
            + "Iceberg table is compatible (think: equal) to 'Operation.PutGlobal.expectedContents'. "
            + "If that is the case, Nessie updates the global state of the Iceberg table to "
            + "'Operation.PutGlobal.contents' and the per-branch state to 'Operation.Put.contents'.")
@Value.Immutable(prehash = true)
@JsonSerialize(as = ImmutableIcebergTable.class)
@JsonDeserialize(as = ImmutableIcebergTable.class)
@JsonTypeName("ICEBERG_TABLE")
public abstract class IcebergTable extends GlobalContents {

  @NotNull
  public abstract IcebergTableState getState();

  public static IcebergTable of(IcebergTableState state) {
    return ImmutableIcebergTable.builder().state(state).build();
  }
}
  
