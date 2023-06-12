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

import static org.projectnessie.quarkus.config.VersionStoreConfig.VersionStoreType.MONGO;

import com.mongodb.client.MongoClient;
import io.quarkus.arc.Arc;
import io.quarkus.mongodb.runtime.MongoClientBeanUtil;
import io.quarkus.mongodb.runtime.MongoClients;
import jakarta.enterprise.context.Dependent;
import jakarta.inject.Inject;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.projectnessie.versioned.persist.adapter.DatabaseAdapter;
import org.projectnessie.versioned.persist.mongodb.MongoClientConfig;
import org.projectnessie.versioned.persist.mongodb.MongoDatabaseAdapterFactory;
import org.projectnessie.versioned.persist.mongodb.MongoDatabaseClient;
import org.projectnessie.versioned.persist.nontx.NonTransactionalDatabaseAdapterConfig;

/** Version store factory for the MongoDB Database Adapter. */
@StoreType(MONGO)
@Dependent
public class MongoDatabaseAdapterBuilder implements DatabaseAdapterBuilder {
  @Inject
  @ConfigProperty(name = "quarkus.mongodb.database")
  String databaseName;

  @Inject NonTransactionalDatabaseAdapterConfig config;

  @Override
  public DatabaseAdapter newDatabaseAdapter() {
    MongoClients mongoClients = Arc.container().instance(MongoClients.class).get();
    MongoClient mongoClient =
        mongoClients.createMongoClient(MongoClientBeanUtil.DEFAULT_MONGOCLIENT_NAME);

    MongoDatabaseClient client = new MongoDatabaseClient();
    client.configure(MongoClientConfig.of(mongoClient).withDatabaseName(databaseName));
    client.initialize();

    return new MongoDatabaseAdapterFactory()
        .newBuilder()
        .withConfig(config)
        .withConnector(client)
        .build();
  }
}
