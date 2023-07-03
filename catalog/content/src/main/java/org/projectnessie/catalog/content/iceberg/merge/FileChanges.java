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

import static com.google.common.base.Preconditions.checkArgument;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import org.apache.iceberg.ContentFile;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.io.FileIO;

class FileChanges {
  final Map<CharSequence, DataFile> addedDataFiles = new LinkedHashMap<>();
  final Map<CharSequence, DeleteFile> addedDeleteFiles = new LinkedHashMap<>();
  final Map<CharSequence, DataFile> removedDataFiles = new LinkedHashMap<>();
  final Map<CharSequence, DeleteFile> removedDeleteFiles = new LinkedHashMap<>();

  /**
   * Construct changes to eventually apply from the changes of the
   * <em>merge-source</em>-to-<em>merge-base</em> minus the changes of the
   * <em>merge-target</em>-to-<em>merge-base</em>.
   *
   * <p>The resulting file sets will contain:
   *
   * <ul>
   *   <li>{@link #addedDataFiles}: the data files added on {@code source}-diff and not added on
   *       {@code target}-diff.
   *   <li>{@link #removedDataFiles}: the data files added on {@code source}-diff and not added on
   *       {@code target}-diff. Data files that are added on the source-diff but appear as removed
   *       on the target-diff cause an {@link IllegalArgumentException}.
   *   <li>{@link #addedDeleteFiles}: the delete files added on {@code source}-diff and not added on
   *       {@code target}-diff. Delete files that are added on the source-diff but appear as removed
   *       on the target-diff cause an {@link IllegalArgumentException}.
   * </ul>
   */
  FileChanges(FileChanges source, FileChanges target) {
    source.addedDataFiles.entrySet().stream()
        .filter(e -> !target.addedDataFiles.containsKey(e.getKey()))
        .forEach(e -> addedDataFiles.put(e.getKey(), e.getValue()));

    target.addedDataFiles.entrySet().stream()
        .filter(e -> !source.addedDataFiles.containsKey(e.getKey()))
        .forEach(e -> removedDataFiles.put(e.getKey(), e.getValue()));

    source.removedDataFiles.entrySet().stream()
        .filter(e -> !target.removedDataFiles.containsKey(e.getKey()))
        .peek(
            e ->
                checkArgument(
                    !target.addedDataFiles.containsKey(e.getKey()),
                    "Data file %s removed by source but added on target."))
        .forEach(e -> removedDataFiles.put(e.getKey(), e.getValue()));

    target.removedDataFiles.entrySet().stream()
        .filter(e -> !source.addedDataFiles.containsKey(e.getKey()))
        .forEach(e -> addedDataFiles.put(e.getKey(), e.getValue()));

    source.addedDeleteFiles.entrySet().stream()
        .filter(e -> !target.addedDeleteFiles.containsKey(e.getKey()))
        .forEach(e -> addedDeleteFiles.put(e.getKey(), e.getValue()));

    target.addedDeleteFiles.entrySet().stream()
        .filter(e -> !source.addedDeleteFiles.containsKey(e.getKey()))
        .forEach(e -> removedDeleteFiles.put(e.getKey(), e.getValue()));

    source.removedDeleteFiles.entrySet().stream()
        .filter(e -> !target.removedDeleteFiles.containsKey(e.getKey()))
        .peek(
            e ->
                checkArgument(
                    !target.addedDeleteFiles.containsKey(e.getKey()),
                    "Delete file %s removed by source but added on target."))
        .forEach(e -> removedDeleteFiles.put(e.getKey(), e.getValue()));

    target.removedDeleteFiles.entrySet().stream()
        .filter(e -> !source.addedDeleteFiles.containsKey(e.getKey()))
        .forEach(e -> addedDeleteFiles.put(e.getKey(), e.getValue()));
  }

  /**
   * Constructs the diff of the data/delete files between the <em>merge-base</em> and the
   * <em>merge-source</em> or <em>merge-target</em>.
   */
  FileChanges(FileIO fileIO, AllFiles mergeBaseFiles, TableMetadata metadata) {
    Set<CharSequence> allDataFiles = new HashSet<>();
    Set<CharSequence> allDeleteFiles = new HashSet<>();

    ManifestScan manifestScan = new ManifestScan(fileIO, metadata.specsById());
    Snapshot snapshot = metadata.currentSnapshot();
    if (snapshot != null) {
      for (ManifestFile manifest : snapshot.allManifests(fileIO)) {
        // Handles both DATA + DELETE manifests.
        manifestScan.scanManifest(
            manifest,
            dataFile -> {
              CharSequence path = dataFile.path();
              allDataFiles.add(path);
              if (!mergeBaseFiles.dataFiles.containsKey(path)) {
                addedDataFiles.put(path, dataFile);
              }
            },
            deleteFile -> {
              CharSequence path = deleteFile.path();
              allDeleteFiles.add(path);
              if (!mergeBaseFiles.deleteFiles.containsKey(path)) {
                addedDeleteFiles.put(path, deleteFile);
              }
            });
      }

      for (Map.Entry<CharSequence, DataFile> baseData : mergeBaseFiles.dataFiles.entrySet()) {
        CharSequence path = baseData.getKey();
        if (!allDataFiles.contains(path)) {
          removedDataFiles.put(path, baseData.getValue());
        }
      }

      for (Map.Entry<CharSequence, DeleteFile> baseDelete : mergeBaseFiles.deleteFiles.entrySet()) {
        CharSequence path = baseDelete.getKey();
        if (!allDeleteFiles.contains(path)) {
          removedDeleteFiles.put(path, baseDelete.getValue());
        }
      }
    }
  }

  boolean isEmpty() {
    return addedDataFiles.isEmpty()
        && removedDataFiles.isEmpty()
        && addedDeleteFiles.isEmpty()
        && removedDeleteFiles.isEmpty();
  }

  public String describe() {
    StringWriter sw = new StringWriter();
    PrintWriter pw = new PrintWriter(sw);
    pw.println("FileChanges:");
    pw.println("  Added data files:");
    for (Map.Entry<CharSequence, DataFile> data : addedDataFiles.entrySet()) {
      pw.println("    File: " + data.getKey());
      pw.println("      " + fileInfo(data.getValue()));
    }
    pw.println("  Removed data files:");
    for (Map.Entry<CharSequence, DataFile> data : removedDataFiles.entrySet()) {
      pw.println("    File: " + data.getKey());
      pw.println("      " + fileInfo(data.getValue()));
    }
    pw.println("  Added delete files:");
    for (Map.Entry<CharSequence, DeleteFile> delete : addedDeleteFiles.entrySet()) {
      pw.println("    File: " + delete.getKey());
      pw.println("      " + fileInfo(delete.getValue()));
    }
    pw.println("  Removed delete files:");
    for (Map.Entry<CharSequence, DeleteFile> delete : removedDeleteFiles.entrySet()) {
      pw.println("    File: " + delete.getKey());
      pw.println("      " + fileInfo(delete.getValue()));
    }
    return sw.toString();
  }

  private static String fileInfo(ContentFile<?> file) {
    return file.format()
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
        + file.fileSequenceNumber();
  }

  FileChanges computeDiffToTarget(FileChanges targetFileChanges) {
    return new FileChanges(this, targetFileChanges);
  }
}
