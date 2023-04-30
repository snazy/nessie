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

import static org.projectnessie.quarkus.config.VersionStoreConfig.VersionStoreType.ROCKS;

import jakarta.enterprise.context.Dependent;
import jakarta.inject.Inject;
import org.projectnessie.versioned.persist.adapter.DatabaseAdapter;
import org.projectnessie.versioned.persist.nontx.NonTransactionalDatabaseAdapterConfig;
import org.projectnessie.versioned.persist.rocks.RocksDatabaseAdapterFactory;
import org.projectnessie.versioned.persist.rocks.RocksDbInstance;

/** RocksDB version store factory. */
@StoreType(ROCKS)
@Dependent
public class RocksDatabaseAdapterBuilder implements DatabaseAdapterBuilder {
  @Inject RocksDbInstance rocksDbInstance;
  @Inject NonTransactionalDatabaseAdapterConfig config;

  @Override
  public DatabaseAdapter newDatabaseAdapter() {
    return new RocksDatabaseAdapterFactory()
        .newBuilder()
        .withConfig(config)
        .withConnector(rocksDbInstance)
        .build();
  }
}
