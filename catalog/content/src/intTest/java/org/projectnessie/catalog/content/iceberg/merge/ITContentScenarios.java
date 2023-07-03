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

import static java.lang.String.format;
import static org.junit.jupiter.params.provider.Arguments.arguments;
import static org.projectnessie.catalog.content.iceberg.merge.ContentScenario.SQL.sql;
import static org.projectnessie.catalog.content.iceberg.merge.ContentScenario.ShowChanges.showChanges;

import java.io.IOException;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.stream.Stream;
import org.apache.iceberg.BaseTable;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.ManifestFiles;
import org.apache.iceberg.ManifestReader;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableOperations;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.io.FileIO;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * Generates various scenarios to simulate merges. Every merge gets a pointer to a table metadata
 * for the <em>merge-base</em>, <em>merge-source</em> and <em>merge-target</em>.
 */
public class ITContentScenarios extends BaseSparkTest {

  @ParameterizedTest
  @MethodSource("contentScenarios")
  public void contentScenario(@SuppressWarnings("unused") String name, ContentScenario scenario) {
    Catalog catalog = icebergCatalog();

    Map<String, TableMetadata> metadataMap = new LinkedHashMap<>();

    FileIO io = null;

    for (ContentScenario.DataAction action : scenario.actions()) {
      System.err.println();

      if (action instanceof ContentScenario.SQL) {
        String sql =
            format(((ContentScenario.SQL) action).sql(), CATALOG_NAME, scenario.tableIdentifier());
        System.err.println("EXEC SQL: " + sql);
        if (action.id() != null) {
          System.err.println("    ID: " + action.id());
        }
        for (Object[] row : execSql(sql)) {
          System.err.println("ROW: " + Arrays.toString(row));
        }

        Table table = catalog.loadTable(scenario.tableIdentifier());
        TableOperations ops = ((BaseTable) table).operations();
        TableMetadata tableMetadata = ops.current();
        System.err.println(tableMetadata.metadataFileLocation());
        if (action.id() != null) {
          metadataMap.put(action.id(), tableMetadata);
        }
        io = table.io();

        showMetadata(io, "(last)", tableMetadata);
      }

      if (action instanceof ContentScenario.ShowChanges) {
        ContentScenario.ShowChanges show = (ContentScenario.ShowChanges) action;
        TableMetadata mergeBase = metadataMap.get(show.mergeBase());
        TableMetadata source = metadataMap.get(show.source());
        TableMetadata target = metadataMap.get(show.target());

        AllFiles allFiles = new AllFiles(io, mergeBase);
        FileChanges sourceChanges = allFiles.computeChanges(source);
        FileChanges targetChanges = allFiles.computeChanges(target);
        FileChanges diffToApply = sourceChanges.computeDiffToTarget(targetChanges);

        System.err.println(
            "SHOW CHANGES, base="
                + show.mergeBase()
                + ", source="
                + show.source()
                + ", target="
                + show.target());
        showMetadata(io, show.mergeBase(), mergeBase);
        showMetadata(io, show.source(), source);
        showMetadata(io, show.target(), target);
        System.err.println("BASE TO SOURCE:");
        System.err.println(sourceChanges.describe());
        System.err.println("BASE TO TARGET:");
        System.err.println(targetChanges.describe());
        System.err.println("TO APPLY:");
        System.err.println(diffToApply.describe());
      }
    }
  }

  private void showMetadata(FileIO io, String id, TableMetadata metadata) {
    System.err.println("METADATA FOR " + id);
    System.err.println(
        "  spec:"
            + metadata.defaultSpecId()
            + ", sort-order:"
            + metadata.defaultSortOrderId()
            + " last-seq:"
            + metadata.lastSequenceNumber());
    Snapshot snapshot = metadata.currentSnapshot();
    if (snapshot != null) {
      for (ManifestFile manifest : snapshot.allManifests(io)) {
        System.err.println("  manifest: " + manifest.path());
        System.err.println(
            "      "
                + manifest.content()
                + ", seq:"
                + manifest.sequenceNumber()
                + ", min-seq:"
                + manifest.minSequenceNumber()
                + ", spec:"
                + manifest.partitionSpecId()
                + ", snapshot:"
                + manifest.snapshotId());
        System.err.println(
            "      added:"
                + manifest.addedFilesCount()
                + "/"
                + manifest.addedRowsCount()
                + ", deleted:"
                + manifest.deletedFilesCount()
                + "/"
                + manifest.deletedRowsCount()
                + ", existing:"
                + manifest.existingFilesCount()
                + "/"
                + manifest.existingRowsCount());
        switch (manifest.content()) {
          case DATA:
            try (ManifestReader<DataFile> reader =
                ManifestFiles.read(manifest, io, metadata.specsById())) {
              for (DataFile file : reader) {
                System.err.println("    data file: " + file.path());
                System.err.println(
                    "      "
                        + file.format()
                        + " spec:"
                        + file.specId()
                        + " sort-order:"
                        + file.sortOrderId()
                        + " partition:"
                        + file.partition()
                        + " pos:"
                        + file.pos()
                        + " data-seq:"
                        + file.dataSequenceNumber()
                        + " file-seq:"
                        + file.fileSequenceNumber());
              }
            } catch (IOException e) {
              throw new RuntimeException(e);
            }
            break;
          case DELETES:
            try (ManifestReader<DeleteFile> reader =
                ManifestFiles.readDeleteManifest(manifest, io, metadata.specsById())) {
              for (DeleteFile file : reader) {
                System.err.println("     delete file: " + file.path());
                System.err.println(
                    "      "
                        + file.format()
                        + " spec:"
                        + file.specId()
                        + " sort-order:"
                        + file.sortOrderId()
                        + " partition:"
                        + file.partition()
                        + " pos:"
                        + file.pos()
                        + " data-seq:"
                        + file.dataSequenceNumber()
                        + " file-seq:"
                        + file.fileSequenceNumber());
              }
            } catch (IOException e) {
              throw new RuntimeException(e);
            }
            break;
        }
      }
    }
  }

  protected static Stream<Arguments> contentScenarios() {
    return Stream.of(
        //
        arguments(
            "deletes+optimize",
            ContentScenario.builder()
                .addActions(
                    sql(
                        "create",
                        "CREATE TABLE %s.%s (part int, num int, name string) USING iceberg "
                            + "PARTITIONED BY (part) "
                            + "TBLPROPERTIES ("
                            + "'format-version'='2', "
                            + "'write.delete.mode'='merge-on-read', "
                            + "'write.update.mode'='merge-on-read', "
                            + "'write.merge.mode'='merge-on-read')"))
                .addActions(
                    sql(
                        "insert",
                        "INSERT INTO %s.%s (part, num, name) VALUES "
                            + "(1, 1, 'one-1'), (1, 2, 'one-2'), (1, 3, 'one-3'), "
                            + "(2, 1, 'two-1'), (2, 2, 'two-2'), (2, 3, 'two-3'), "
                            + "(3, 1, 'three-1'), (3, 2, 'three-2'), (3, 3, 'three-3')"))
                .addActions(sql("delete_partition", "DELETE FROM %s.%s WHERE part = 1"))
                .addActions(sql("delete_eq", "DELETE FROM %s.%s WHERE part = 2 AND num = 2"))
                .addActions(
                    sql(
                        "insert2",
                        "INSERT INTO %s.%s (part, num, name) VALUES "
                            + "(1, 4, 'one-4'), (1, 5, 'one-5'), (1, 6, 'one-6')"))
                .addActions(sql("delete_range", "DELETE FROM %s.%s WHERE part = 3 AND num >= 2"))
                .addActions(showChanges("insert", "delete_range", "insert"))
                .addActions(
                    sql(
                        "CALL %s.system.rewrite_data_files(table => '%s', options => map('rewrite-all','true'))"))
                .addActions(
                    sql(
                        "rewrite",
                        "CALL %s.system.rewrite_position_delete_files(table => '%s', options => map('rewrite-all','true'))"))
                .addActions(showChanges("insert", "rewrite", "delete_range"))
                .addActions(showChanges("create", "rewrite", "delete_range"))
                .addActions(showChanges("create", "rewrite", "insert"))
                .addActions(showChanges("create", "delete_range", "insert"))
                .build()),
        //
        arguments(
            "updates+optimize",
            ContentScenario.builder()
                .addActions(
                    sql(
                        "create",
                        "CREATE TABLE %s.%s (part int, num int, name string) USING iceberg "
                            + "PARTITIONED BY (part) "
                            + "TBLPROPERTIES ("
                            + "'format-version'='2', "
                            + "'write.delete.mode'='merge-on-read', "
                            + "'write.update.mode'='merge-on-read', "
                            + "'write.merge.mode'='merge-on-read')"))
                .addActions(
                    sql(
                        "insert",
                        "INSERT INTO %s.%s (part, num, name) VALUES "
                            + "(1, 1, 'one-1'), (1, 2, 'one-2'), (1, 3, 'one-3'), "
                            + "(2, 1, 'two-1'), (2, 2, 'two-2'), (2, 3, 'two-3'), "
                            + "(3, 1, 'three-1'), (3, 2, 'three-2'), (3, 3, 'three-3')"))
                .addActions(sql("update_partition", "UPDATE %s.%s SET name = 'foo' WHERE part = 1"))
                .addActions(
                    sql("update_eq", "UPDATE %s.%s SET name = 'bar' WHERE part = 2 AND num = 2"))
                .addActions(
                    sql(
                        "insert2",
                        "INSERT INTO %s.%s (part, num, name) VALUES "
                            + "(1, 4, 'one-4'), (1, 5, 'one-5'), (1, 6, 'one-6')"))
                .addActions(sql("update_range", "DELETE FROM %s.%s WHERE part = 3 AND num >= 2"))
                .addActions(showChanges("insert", "update_range", "insert"))
                .addActions(
                    sql(
                        "CALL %s.system.rewrite_data_files(table => '%s', options => map('rewrite-all','true'))"))
                .addActions(
                    sql(
                        "rewrite",
                        "CALL %s.system.rewrite_position_delete_files(table => '%s', options => map('rewrite-all','true'))"))
                .addActions(showChanges("insert", "rewrite", "update_range"))
                .addActions(showChanges("create", "rewrite", "update_range"))
                .addActions(showChanges("create", "rewrite", "insert"))
                .addActions(showChanges("create", "update_range", "insert"))
                .build())
        //
        );
  }
}
