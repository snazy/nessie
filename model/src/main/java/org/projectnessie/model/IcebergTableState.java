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

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import javax.validation.constraints.NotNull;
import org.eclipse.microprofile.openapi.annotations.enums.SchemaType;
import org.eclipse.microprofile.openapi.annotations.media.Schema;
import org.immutables.value.Value;

/**
 * To ensure that different branches have a consistent view of the Iceberg-table-attributes that
 * need to be globally unique, a commit-operation for an Iceberg table consists of a {@link
 * Operation.Put} operation with an {@link IcebergSnapshot} <em>and</em> a {@link
 * Operation.PutGlobal} with an {@link IcebergTable}.
 *
 * <p>During a commit-operation, Nessie checks whether the known global state of the Iceberg table
 * is compatible (think: equal) to {@link Operation.PutGlobal#getExpectedContents()}. If that is the
 * case, Nessie updates the global state of the Iceberg table to {@link
 * Operation.PutGlobal#getContents()} and the per-branch state to {@link
 * Operation.Put#getContents()}.
 */
@Schema(
    type = SchemaType.OBJECT,
    title = "Iceberg table snapshot",
    description =
        "To ensure that different branches have a consistent view of the Iceberg-table-attributes "
            + "that need to be globally unique, a commit-operation for an Iceberg table consists of "
            + "an 'Operation.Put' operation with an 'IcebergSnapshot' and an 'Operation.PutGlobal' "
            + "with an IcebergTable.\n"
            + "\n"
            + "During a commit-operation, Nessie checks whether the known global state of the "
            + "Iceberg table is compatible (think: equal) to 'Operation.PutGlobal.expectedContents'. "
            + "If that is the case, Nessie updates the global state of the Iceberg table to "
            + "'Operation.PutGlobal.contents' and the per-branch state to 'Operation.Put.contents'.")
@Value.Immutable(prehash = true)
@JsonSerialize(as = ImmutableIcebergTableState.class)
@JsonDeserialize(as = ImmutableIcebergTableState.class)
public abstract class IcebergTableState {

  /** Reflects Iceberg's {@code TableMetadata.lastColumnId()}. */
  @NotNull
  public abstract Integer getLastColumnId();

  /** Reflects Iceberg's {@code TableMetadata.currentSchemaId()}. */
  @NotNull
  public abstract Integer getCurrentSchemaId();

  /** Reflects Iceberg's {@code TableMetadata.lastAssignedPartitionId()}. */
  @NotNull
  public abstract Integer getLastAssignedPartitionId();

  /** Reflects Iceberg's {@code TableMetadata.currentSchemaId()}. */
  @NotNull
  public abstract Long getCurrentSnapshotId();

  public static IcebergTableState of(
      int lastColumnId, int currentSchemaId, int lastAssignedPartitionId, long currentSnapshotId) {
    return ImmutableIcebergTableState.builder()
        .lastColumnId(lastColumnId)
        .currentSchemaId(currentSchemaId)
        .lastAssignedPartitionId(lastAssignedPartitionId)
        .currentSnapshotId(currentSnapshotId)
        .build();
  }
}
