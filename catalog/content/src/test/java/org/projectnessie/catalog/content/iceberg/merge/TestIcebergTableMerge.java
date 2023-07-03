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

import static com.google.common.collect.Lists.newArrayList;
import static java.util.Collections.emptyList;
import static org.assertj.core.api.InstanceOfAssertFactories.list;
import static org.junit.jupiter.params.provider.Arguments.arguments;

import java.nio.file.Path;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.iceberg.BaseTable;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.DeleteFiles;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.SortOrder;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.types.Types;
import org.assertj.core.api.SoftAssertions;
import org.assertj.core.api.junit.jupiter.InjectSoftAssertions;
import org.assertj.core.api.junit.jupiter.SoftAssertionsExtension;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.projectnessie.catalog.content.iceberg.LocalFileIO;
import org.projectnessie.catalog.content.iceberg.ops.CatalogTableOperations;

@ExtendWith(SoftAssertionsExtension.class)
public class TestIcebergTableMerge {
  @InjectSoftAssertions protected SoftAssertions soft;

  @TempDir static Path warehouseDir;

  static FileIO fileIO;

  @BeforeAll
  public static void setup() {
    fileIO = new LocalFileIO();
  }

  @ParameterizedTest
  @MethodSource("testData")
  public void merge(
      TableMetadata sourceMetadata,
      TableMetadata targetMetadata,
      TableMetadata mergeBaseMetadata,
      List<Snapshot> expectedSnapshotsOnSource)
      throws Exception {
    String tableName = "foo_table";
    CatalogTableOperations catalogTableOperations = new CatalogTableOperations(fileIO, tableName);

    IcebergTableMerge merge =
        IcebergTableMerge.builder()
            .sourceTableMetadata(sourceMetadata)
            .targetTableMetadata(targetMetadata)
            .mergeBaseTableMetadata(mergeBaseMetadata)
            .tableName(tableName)
            .ops(catalogTableOperations)
            .build();

    Optional<TableMetadata> merged = merge.merge();
    if (expectedSnapshotsOnSource.isEmpty()) {
      soft.assertThat(merged).isEmpty();
    } else {
      Set<CharSequence> allRemoved =
          expectedSnapshotsOnSource.stream()
              .flatMap(s -> newArrayList(s.removedDataFiles(fileIO)).stream())
              .map(DataFile::path)
              .collect(Collectors.toSet());
      List<CharSequence> allAdded =
          expectedSnapshotsOnSource.stream()
              .flatMap(s -> newArrayList(s.addedDataFiles(fileIO)).stream())
              .map(DataFile::path)
              .filter(p -> !allRemoved.contains(p))
              .collect(Collectors.toList());
      soft.assertThat(merged)
          .get()
          .extracting(TableMetadata::currentSnapshot)
          .extracting(s -> newArrayList(s.addedDataFiles(fileIO)), list(DataFile.class))
          .extracting(DataFile::path)
          .containsExactlyInAnyOrderElementsOf(allAdded);
      // soft.assertThat(merged)
      //    .get()
      //    .extracting(TableMetadata::currentSnapshot)
      //    .extracting(s -> newArrayList(s.removedDataFiles(fileIO)),
      // list(DataFile.class))
      //    .extracting(DataFile::path)
      //    .containsExactlyElementsOf(allRemoved);
      soft.assertThat(merged)
          .get()
          .extracting(
              TableMetadata::currentSchemaId,
              TableMetadata::defaultSortOrderId,
              TableMetadata::defaultSpecId)
          .containsExactly(
              sourceMetadata.currentSchemaId(),
              sourceMetadata.defaultSortOrderId(),
              sourceMetadata.defaultSpecId());
    }
  }

  static Stream<Arguments> testData() {
    Schema schema = new Schema(1, emptyList());
    Schema schema2 =
        new Schema(
            2,
            Types.NestedField.of(0, true, "col_0", Types.StringType.get()),
            Types.NestedField.of(1, true, "col_1", Types.StringType.get()));
    Schema schema3 =
        new Schema(
            2,
            Types.NestedField.of(1, true, "col_1", Types.StringType.get()),
            Types.NestedField.of(3, true, "col_3", Types.StringType.get()),
            Types.NestedField.of(5, true, "col_5", Types.StringType.get()));
    Schema schema4 =
        new Schema(
            3,
            Types.NestedField.of(1, true, "col_1", Types.StringType.get()),
            Types.NestedField.of(3, true, "col_3", Types.StringType.get()));
    PartitionSpec partitionSpec = PartitionSpec.unpartitioned();
    SortOrder sortOrder = SortOrder.unsorted();

    TableMetadata root =
        TableMetadata.buildFromEmpty()
            .addSchema(schema, schema.highestFieldId())
            .addSchema(schema2, schema2.highestFieldId())
            .setCurrentSchema(schema3, schema3.highestFieldId())
            .addPartitionSpec(partitionSpec)
            .addSortOrder(sortOrder)
            .setLocation(
                warehouseDir
                    .resolve("metadata")
                    .resolve(UUID.randomUUID().toString())
                    .toUri()
                    .toString())
            .assignUUID()
            .build();

    CatalogTableOperations ops = new CatalogTableOperations(fileIO, "foo").setCurrent(root);

    createSnapshot(ops, partitionSpec, 1);
    createSnapshot(ops, partitionSpec, 2);
    Snapshot snapshot3 = createSnapshot(ops, partitionSpec, 3);

    TableMetadata common = ops.current();

    ops.setCurrent(common);
    Snapshot snapshot11 = createSnapshot(ops, partitionSpec, 11);
    Snapshot snapshot12 = createSnapshot(ops, partitionSpec, 12);
    Snapshot snapshot13 = createSnapshot(ops, partitionSpec, 13);
    Snapshot snapshot14 =
        deleteFiles(
            ops,
            snapshot3.addedDataFiles(fileIO).iterator().next(),
            snapshot11.addedDataFiles(fileIO).iterator().next());
    TableMetadata source = ops.current();

    ops.setCurrent(common);
    Snapshot snapshot21 = createSnapshot(ops, partitionSpec, 21);
    Snapshot snapshot22 = createSnapshot(ops, partitionSpec, 22);
    Snapshot snapshot23 = createSnapshot(ops, partitionSpec, 23);
    TableMetadata target = ops.current();

    return Stream.of(
        arguments(
            source,
            target,
            common,
            List.of(snapshot11, snapshot12, snapshot13, snapshot14),
            List.of(snapshot21, snapshot22, snapshot23)),
        arguments(
            source,
            common,
            common,
            List.of(snapshot11, snapshot12, snapshot13, snapshot14),
            List.of()),
        arguments(common, target, common, List.of(), List.of(snapshot21, snapshot22, snapshot23)),
        arguments(common, common, common, List.of(), List.of()));
  }

  private static Snapshot createSnapshot(
      CatalogTableOperations ops, PartitionSpec partitionSpec, int snapshotNum) {
    BaseTable table = new BaseTable(ops, "foo");

    table
        .newAppend()
        .appendFile(
            DataFiles.builder(partitionSpec)
                .withPath("/data/" + snapshotNum + "-1")
                .withRecordCount(1)
                .withFileSizeInBytes(1)
                .withFormat(FileFormat.PARQUET)
                .build())
        .appendFile(
            DataFiles.builder(partitionSpec)
                .withPath("/data/" + snapshotNum + "-2")
                .withRecordCount(2)
                .withFileSizeInBytes(1)
                .withFormat(FileFormat.PARQUET)
                .build())
        .commit();

    return ops.current().currentSnapshot();
  }

  private static Snapshot deleteFiles(CatalogTableOperations ops, DataFile... dataFiles) {
    BaseTable table = new BaseTable(ops, "foo");

    DeleteFiles deleteFiles = table.newDelete();
    for (DataFile dataFile : dataFiles) {
      deleteFiles.deleteFile(dataFile);
    }
    deleteFiles.commit();

    return ops.current().currentSnapshot();
  }
}
