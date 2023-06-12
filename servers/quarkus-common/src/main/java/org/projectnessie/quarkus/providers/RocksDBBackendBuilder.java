/*
 * Copyright (C) 2022 Dremio
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
package org.projectnessie.quarkus.providers;

import static org.projectnessie.quarkus.config.VersionStoreConfig.VersionStoreType.ROCKSDB;

import jakarta.enterprise.context.Dependent;
import jakarta.inject.Inject;
import org.projectnessie.quarkus.config.QuarkusRocksConfig;
import org.projectnessie.versioned.storage.common.persist.Backend;
import org.projectnessie.versioned.storage.rocksdb.RocksDBBackendConfig;
import org.projectnessie.versioned.storage.rocksdb.RocksDBBackendFactory;

@StoreType(ROCKSDB)
@Dependent
public class RocksDBBackendBuilder implements BackendBuilder {

  @Inject QuarkusRocksConfig config;

  @Override
  public Backend buildBackend() {
    RocksDBBackendFactory factory = new RocksDBBackendFactory();
    RocksDBBackendConfig c = RocksDBBackendConfig.builder().from(config).build();
    return factory.buildBackend(c);
  }
}
