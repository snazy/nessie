/*
 * Copyright (C) 2024 Dremio
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
package org.projectnessie.catalog.files.adls;

import static org.projectnessie.catalog.files.adls.AdlsLocation.adlsLocation;

import com.azure.storage.blob.models.BlobStorageException;
import com.azure.storage.blob.models.ParallelTransferOptions;
import com.azure.storage.file.datalake.DataLakeFileClient;
import com.azure.storage.file.datalake.DataLakeFileSystemClient;
import com.azure.storage.file.datalake.options.DataLakeFileInputStreamOptions;
import com.azure.storage.file.datalake.options.DataLakeFileOutputStreamOptions;
import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.projectnessie.catalog.files.api.ObjectIO;
import org.projectnessie.storage.uri.StorageUri;

public class AdlsObjectIO implements ObjectIO {
  private final AdlsClientSupplier clientSupplier;

  public AdlsObjectIO(AdlsClientSupplier clientSupplier) {
    this.clientSupplier = clientSupplier;
  }

  @Override
  public void ping(StorageUri uri) {
    AdlsLocation location = adlsLocation(uri);

    DataLakeFileSystemClient fileSystem = clientSupplier.fileSystemClient(location);
    fileSystem.getProperties();
  }

  @Override
  public InputStream readObject(StorageUri uri) throws IOException {
    DataLakeFileClient file = clientSupplier.fileClientForLocation(uri);
    DataLakeFileInputStreamOptions options = new DataLakeFileInputStreamOptions();
    clientSupplier.adlsOptions().readBlockSize().ifPresent(options::setBlockSize);
    return file.openInputStream(options).getInputStream();
  }

  @Override
  public OutputStream writeObject(StorageUri uri) {
    DataLakeFileClient file = clientSupplier.fileClientForLocation(uri);
    DataLakeFileOutputStreamOptions options = new DataLakeFileOutputStreamOptions();
    ParallelTransferOptions transferOptions = new ParallelTransferOptions();
    clientSupplier.adlsOptions().writeBlockSize().ifPresent(transferOptions::setBlockSizeLong);
    options.setParallelTransferOptions(transferOptions);
    return new BufferedOutputStream(file.getOutputStream(options));
  }

  @Override
  public void deleteObjects(List<StorageUri> uris) throws IOException {
    // Note: the default container is mapped to an empty string
    Map<String, List<AdlsLocation>> bucketToUris =
        uris.stream()
            .map(AdlsLocation::adlsLocation)
            .collect(Collectors.groupingBy(l -> l.container().orElse("")));

    IOException ex = null;
    for (List<AdlsLocation> locations : bucketToUris.values()) {
      DataLakeFileSystemClient fileSystem = clientSupplier.fileSystemClient(locations.get(0));

      // No batch-delete ... yay
      for (AdlsLocation location : locations) {
        String path = location.path();
        if (path.startsWith("/")) {
          path = path.substring(1);
        }
        try {
          fileSystem.deleteFileIfExists(path);
        } catch (BlobStorageException e) {
          if (e.getStatusCode() != 404) {
            if (ex == null) {
              ex = new IOException(e.getServiceMessage(), e);
            } else {
              ex.addSuppressed(e);
            }
          }
        }
      }
    }
    if (ex != null) {
      throw ex;
    }
  }
}
