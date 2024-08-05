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
package org.projectnessie.versioned.storage.foundationdb;

import static org.projectnessie.versioned.storage.common.logic.Logics.repositoryLogic;

import org.assertj.core.api.SoftAssertions;
import org.assertj.core.api.junit.jupiter.InjectSoftAssertions;
import org.assertj.core.api.junit.jupiter.SoftAssertionsExtension;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.projectnessie.versioned.storage.common.config.StoreConfig;
import org.projectnessie.versioned.storage.common.logic.RepositoryDescription;
import org.projectnessie.versioned.storage.common.logic.RepositoryLogic;
import org.projectnessie.versioned.storage.common.persist.Backend;
import org.projectnessie.versioned.storage.common.persist.BackendFactory;
import org.projectnessie.versioned.storage.common.persist.Persist;
import org.projectnessie.versioned.storage.common.persist.PersistFactory;
import org.projectnessie.versioned.storage.common.persist.PersistLoader;
import org.projectnessie.versioned.storage.foundationdbtests.FoundationDBBackendTestFactory;

@ExtendWith(SoftAssertionsExtension.class)
public class ITFoundationDBBackendFactory {
  @InjectSoftAssertions protected SoftAssertions soft;

  /*
  @Test
  public void productionLike() throws Exception {
    FoundationDBBackendTestFactory testFactory = new FoundationDBBackendTestFactory();
    testFactory.start();
    try {
      BackendFactory<FoundationDBBackendConfig> factory =
          PersistLoader.findFactoryByName(FoundationDBBackendFactory.NAME);
      soft.assertThat(factory).isNotNull().isInstanceOf(FoundationDBBackendFactory.class);

      try (FoundationDbClient client = testFactory.buildNewClient()) {
        RepositoryDescription repoDesc;
        try (Backend backend =
            factory.buildBackend(FoundationDBBackendConfig.builder().client(client).build())) {
          soft.assertThat(backend).isNotNull().isInstanceOf(FoundationDBBackend.class);
          backend.setupSchema();
          PersistFactory persistFactory = backend.createFactory();
          soft.assertThat(persistFactory).isNotNull().isInstanceOf(FoundationDBPersistFactory.class);
          Persist persist = persistFactory.newPersist(StoreConfig.Adjustable.empty());
          soft.assertThat(persist).isNotNull().isInstanceOf(FoundationDBPersist.class);

          RepositoryLogic repositoryLogic = repositoryLogic(persist);
          repositoryLogic.initialize("initializeAgain");
          repoDesc = repositoryLogic.fetchRepositoryDescription();
          soft.assertThat(repoDesc).isNotNull();
        }

        try (Backend backend =
            factory.buildBackend(FoundationDBBackendConfig.builder().client(client).build())) {
          soft.assertThat(backend).isNotNull().isInstanceOf(FoundationDBBackend.class);
          backend.setupSchema();
          PersistFactory persistFactory = backend.createFactory();
          soft.assertThat(persistFactory).isNotNull().isInstanceOf(FoundationDBPersistFactory.class);
          Persist persist = persistFactory.newPersist(StoreConfig.Adjustable.empty());
          soft.assertThat(persist).isNotNull().isInstanceOf(FoundationDBPersist.class);

          RepositoryLogic repositoryLogic = repositoryLogic(persist);
          repositoryLogic.initialize("initializeAgain");
          soft.assertThat(repositoryLogic.fetchRepositoryDescription()).isEqualTo(repoDesc);
        }
      }
    } finally {
      testFactory.stop();
    }
  }

  @Test
  public void backendTestFactory() throws Exception {
    FoundationDBBackendTestFactory testFactory = new FoundationDBBackendTestFactory();
    testFactory.start();
    try {
      BackendFactory<FoundationDBBackendConfig> factory =
          PersistLoader.findFactoryByName(FoundationDBBackendFactory.NAME);
      soft.assertThat(factory).isNotNull().isInstanceOf(FoundationDBBackendFactory.class);

      RepositoryDescription repoDesc;
      try (Backend backend = testFactory.createNewBackend()) {
        soft.assertThat(backend).isNotNull().isInstanceOf(FoundationDBBackend.class);
        backend.setupSchema();
        PersistFactory persistFactory = backend.createFactory();
        soft.assertThat(persistFactory).isNotNull().isInstanceOf(FoundationDBPersistFactory.class);
        Persist persist = persistFactory.newPersist(StoreConfig.Adjustable.empty());
        soft.assertThat(persist).isNotNull().isInstanceOf(FoundationDBPersist.class);

        RepositoryLogic repositoryLogic = repositoryLogic(persist);
        repositoryLogic.initialize("initializeAgain");
        repoDesc = repositoryLogic.fetchRepositoryDescription();
        soft.assertThat(repoDesc).isNotNull();
      }

      try (Backend backend = testFactory.createNewBackend()) {
        soft.assertThat(backend).isNotNull().isInstanceOf(FoundationDBBackend.class);
        backend.setupSchema();
        PersistFactory persistFactory = backend.createFactory();
        soft.assertThat(persistFactory).isNotNull().isInstanceOf(FoundationDBPersistFactory.class);
        Persist persist = persistFactory.newPersist(StoreConfig.Adjustable.empty());
        soft.assertThat(persist).isNotNull().isInstanceOf(FoundationDBPersist.class);

        RepositoryLogic repositoryLogic = repositoryLogic(persist);
        repositoryLogic.initialize("initializeAgain");
        soft.assertThat(repositoryLogic.fetchRepositoryDescription()).isEqualTo(repoDesc);
      }
    } finally {
      testFactory.stop();
    }
  }

  @Test
  public void verifyKeySchema() {
    FoundationDBBackendTestFactory testFactory = new FoundationDBBackendTestFactory();
    testFactory.start();
    try {
      try (FoundationDbClient client = testFactory.buildNewClient()) {
        client.createTable(
            b ->
                b.tableName(TABLE_REFS)
                    .attributeDefinitions(
                        AttributeDefinition.builder()
                            .attributeName(KEY_NAME)
                            .attributeType(ScalarAttributeType.S)
                            .build(),
                        AttributeDefinition.builder()
                            .attributeName("l")
                            .attributeType(ScalarAttributeType.S)
                            .build())
                    .billingMode(BillingMode.PAY_PER_REQUEST)
                    .keySchema(
                        KeySchemaElement.builder()
                            .attributeName(KEY_NAME)
                            .keyType(KeyType.HASH)
                            .build(),
                        KeySchemaElement.builder()
                            .attributeName("l")
                            .keyType(KeyType.RANGE)
                            .build()));

        try (FoundationDBBackend backend = testFactory.createNewBackend()) {
          soft.assertThatIllegalStateException()
              .isThrownBy(backend::setupSchema)
              .withMessage(
                  "Invalid key schema for table: %s. "
                      + "Key schema should be a hash partitioned attribute with the name '%s'.",
                  TABLE_REFS, KEY_NAME);
        }

        client.deleteTable(b -> b.tableName(TABLE_REFS));

        String otherCol = "something";
        client.createTable(
            b ->
                b.tableName(TABLE_REFS)
                    .attributeDefinitions(
                        AttributeDefinition.builder()
                            .attributeName(otherCol)
                            .attributeType(ScalarAttributeType.S)
                            .build())
                    .billingMode(BillingMode.PAY_PER_REQUEST)
                    .keySchema(
                        KeySchemaElement.builder()
                            .attributeName(otherCol)
                            .keyType(KeyType.HASH)
                            .build()));

        try (FoundationDBBackend backend = testFactory.createNewBackend()) {
          soft.assertThatIllegalStateException()
              .isThrownBy(backend::setupSchema)
              .withMessage(
                  "Invalid key schema for table: %s. "
                      + "Key schema should be a hash partitioned attribute with the name '%s'.",
                  TABLE_REFS, KEY_NAME);
        }
      }
    } finally {
      testFactory.stop();
    }
  }
  */
}
