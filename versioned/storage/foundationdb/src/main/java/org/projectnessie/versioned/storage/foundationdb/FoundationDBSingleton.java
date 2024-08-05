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

package org.projectnessie.versioned.storage.foundationdb;

import com.apple.foundationdb.ApiVersion;
import com.apple.foundationdb.Database;
import com.apple.foundationdb.FDB;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

final class FoundationDBSingleton {
  private FoundationDBSingleton() {}

  static FoundationDBHandle openCluster(String clusterDescription) {
    if (!FoundationDBHandle.LOCK.tryLock()) {
      throw new IllegalStateException("Only one client at a time allowed");
    }

    try {
      FDB fdb = FDB.selectAPIVersion(ApiVersion.LATEST);

      Path clusterFile;
      try {
        clusterFile = Files.createTempFile("fdb-", "");
        Files.writeString(clusterFile, clusterDescription);
      } catch (IOException e) {
        throw new RuntimeException(e);
      }

      Database database = fdb.open(clusterFile.toAbsolutePath().toString());
      return new FoundationDBHandle(fdb, database);
    } catch (Exception e) {
      try {
        throw new RuntimeException(e);
      } finally {
        FoundationDBHandle.LOCK.unlock();
      }
    }
  }

  static final class FoundationDBHandle implements AutoCloseable {
    private static final Lock LOCK = new ReentrantLock();

    private final FDB fdb;
    private final Database database;

    public FoundationDBHandle(FDB fdb, Database database) {
      this.fdb = fdb;
      this.database = database;
    }

    FDB fdb() {
      return fdb;
    }

    Database getDatabase() {
      return database;
    }

    @Override
    public void close() {
      LOCK.unlock();
    }
  }
}
