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

import java.util.Map;
import java.util.function.Consumer;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.ManifestFiles;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.io.FileIO;

final class ManifestScan {
  final FileIO fileIO;
  final Map<Integer, PartitionSpec> specsById;

  ManifestScan(FileIO fileIO, Map<Integer, PartitionSpec> specsById) {
    this.fileIO = fileIO;
    this.specsById = specsById;
  }

  void scanManifest(
      ManifestFile manifest,
      Consumer<DataFile> dataFileConsumer,
      Consumer<DeleteFile> deleteFileConsumer) {
    switch (manifest.content()) {
      case DATA:
        for (DataFile dataFile : ManifestFiles.read(manifest, fileIO, specsById)) {
          switch (dataFile.content()) {
            case DATA:
              dataFileConsumer.accept(dataFile);
              break;
            case POSITION_DELETES:
            case EQUALITY_DELETES:
              throw new IllegalStateException(
                  "Unsupported Iceberg DataFile content " + dataFile.content());
            default:
              throw new IllegalStateException(
                  "Unknown Iceberg DataFile content " + dataFile.content());
          }
        }
        break;
      case DELETES:
        for (DeleteFile deleteFile :
            ManifestFiles.readDeleteManifest(manifest, fileIO, specsById)) {
          switch (deleteFile.content()) {
            case POSITION_DELETES:
            case EQUALITY_DELETES:
              deleteFileConsumer.accept(deleteFile);
              break;
            case DATA:
              throw new IllegalStateException(
                  "Unsupported Iceberg DeleteFile content " + deleteFile.content());
            default:
              throw new IllegalStateException(
                  "Unknown Iceberg DeleteFile content " + deleteFile.content());
          }
        }
        break;
      default:
        throw new IllegalStateException("Unknown Iceberg Manifest content " + manifest.content());
    }
  }
}
