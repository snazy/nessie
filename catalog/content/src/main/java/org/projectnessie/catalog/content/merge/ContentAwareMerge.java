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

import java.io.IOException;
import java.util.Optional;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.io.FileIO;
import org.projectnessie.catalog.content.iceberg.merge.IcebergTableMerge;
import org.projectnessie.catalog.content.iceberg.metadata.MetadataIO;
import org.projectnessie.catalog.content.iceberg.ops.CatalogTableOperations;
import org.projectnessie.model.Content;
import org.projectnessie.model.IcebergTable;

public class ContentAwareMerge {

  public static <C extends Content> Optional<C> mergeContent(ContentMerge<C> contentMerge)
      throws IOException {
    if (contentMerge.contentType().equals(Content.Type.ICEBERG_TABLE)) {
      @SuppressWarnings("unchecked")
      ContentMerge<IcebergTable> icebergMerge = (ContentMerge<IcebergTable>) contentMerge;
      @SuppressWarnings("unchecked")
      Optional<C> r = (Optional<C>) mergeIcebergTable(icebergMerge);
      return r;
    }

    return Optional.empty();
  }

  private static Optional<IcebergTable> mergeIcebergTable(ContentMerge<IcebergTable> contentMerge)
      throws IOException {
    MetadataIO metadataIO = contentMerge.metadataIO();
    TableMetadata mergeBaseTableMetadata =
        metadataIO.loadAndFixTableMetadata(contentMerge.mergeBaseTable());
    TableMetadata sourceTableMetadata =
        metadataIO.loadAndFixTableMetadata(contentMerge.sourceTable());
    TableMetadata targetTableMetadata =
        metadataIO.loadAndFixTableMetadata(contentMerge.targetTable());

    String tableName = contentMerge.sourceKey().getName();
    FileIO fileIO = contentMerge.fileIO();
    CatalogTableOperations ops = new CatalogTableOperations(fileIO, tableName);

    IcebergTableMerge tableMerge =
        IcebergTableMerge.builder()
            .tableName(tableName)
            .ops(ops)
            .mergeBaseTableMetadata(mergeBaseTableMetadata)
            .sourceTableMetadata(sourceTableMetadata)
            .targetTableMetadata(targetTableMetadata)
            .build();
    Optional<TableMetadata> resolvedMetadata = tableMerge.merge();

    if (resolvedMetadata.isEmpty()) {
      return Optional.empty();
    }

    TableMetadata resolved = resolvedMetadata.get();

    int newVersion = metadataIO.parseVersion(contentMerge.targetTable().getMetadataLocation()) + 1;
    resolved = metadataIO.store(resolved, newVersion);

    return Optional.of(
        IcebergTable.of(
            resolved.metadataFileLocation(),
            resolved.currentSnapshot().snapshotId(),
            resolved.currentSchemaId(),
            resolved.defaultSpecId(),
            resolved.defaultSortOrderId(),
            contentMerge.sourceTable().getId()));
  }
}
