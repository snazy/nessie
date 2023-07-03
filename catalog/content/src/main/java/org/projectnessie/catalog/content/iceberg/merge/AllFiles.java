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

import java.util.LinkedHashMap;
import java.util.Map;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.io.FileIO;

class AllFiles {
  final FileIO fileIO;

  final Map<CharSequence, DataFile> dataFiles = new LinkedHashMap<>();
  final Map<CharSequence, DeleteFile> deleteFiles = new LinkedHashMap<>();

  AllFiles(FileIO fileIO, TableMetadata metadata) {
    this.fileIO = fileIO;
    ManifestScan manifestScan = new ManifestScan(fileIO, metadata.specsById());
    Snapshot snapshot = metadata.currentSnapshot();
    if (snapshot != null) {
      for (ManifestFile manifest : snapshot.allManifests(fileIO)) {
        manifestScan.scanManifest(
            manifest,
            dataFile -> {
              CharSequence path = dataFile.path();
              dataFiles.put(path, dataFile);
            },
            deleteFile -> {
              CharSequence path = deleteFile.path();
              deleteFiles.put(path, deleteFile);
            });
      }
    }
  }

  FileChanges computeChanges(TableMetadata metadata) {
    return new FileChanges(fileIO, this, metadata);
  }
}
