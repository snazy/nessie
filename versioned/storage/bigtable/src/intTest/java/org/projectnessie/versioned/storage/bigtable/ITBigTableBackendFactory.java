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
package org.projectnessie.versioned.storage.bigtable;

import static org.projectnessie.versioned.storage.bigtable.BigTableConstants.FAMILY_OBJS;
import static org.projectnessie.versioned.storage.bigtable.BigTableConstants.FAMILY_REFS;
import static org.projectnessie.versioned.storage.bigtable.BigTableConstants.GCRULE_MAX_VERSIONS_1;
import static org.projectnessie.versioned.storage.bigtable.BigTableConstants.TABLE_OBJS;
import static org.projectnessie.versioned.storage.bigtable.BigTableConstants.TABLE_REFS;
import static org.projectnessie.versioned.storage.common.logic.Logics.repositoryLogic;

import com.google.cloud.bigtable.admin.v2.BigtableTableAdminClient;
import com.google.cloud.bigtable.admin.v2.models.ColumnFamily;
import com.google.cloud.bigtable.admin.v2.models.CreateTableRequest;
import com.google.cloud.bigtable.data.v2.BigtableDataClient;
import org.assertj.core.api.SoftAssertions;
import org.assertj.core.api.junit.jupiter.InjectSoftAssertions;
import org.assertj.core.api.junit.jupiter.SoftAssertionsExtension;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
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

@ExtendWith(SoftAssertionsExtension.class)
public class ITBigTableBackendFactory {
  @InjectSoftAssertions protected SoftAssertions soft;

  static StoreConfig DEFAULT_CONFIG = new StoreConfig() {};

  AbstractBigTableBackendTestFactory testFactory;

  @BeforeEach
  void startTestFactory() throws Exception {
    testFactory = new BigTableBackendContainerTestFactory();
    testFactory.start();
  }

  @AfterEach
  void stopTestFactory() throws Exception {
    try {
      if (testFactory != null) {
        testFactory.stop();
      }
    } finally {
      testFactory = null;
    }
  }

  @Test
  public void modifyGcRule() throws Exception {

    try (BigtableDataClient dataClient = testFactory.buildNewDataClient();
        BigtableTableAdminClient tableAdminClient = testFactory.buildNewTableAdminClient()) {
      soft.assertThat(tableAdminClient.listTables()).isEmpty();

      tableAdminClient.createTable(CreateTableRequest.of(TABLE_REFS).addFamily(FAMILY_REFS));
      tableAdminClient.createTable(CreateTableRequest.of(TABLE_OBJS).addFamily(FAMILY_OBJS));

      soft.assertThat(
              tableAdminClient.getTable(TABLE_REFS).getColumnFamilies().stream()
                  .filter(cf -> cf.getId().equals(FAMILY_REFS))
                  .findFirst())
          .get()
          .extracting(ColumnFamily::hasGCRule)
          .isEqualTo(Boolean.FALSE);
      soft.assertThat(
              tableAdminClient.getTable(TABLE_OBJS).getColumnFamilies().stream()
                  .filter(cf -> cf.getId().equals(FAMILY_OBJS))
                  .findFirst())
          .get()
          .extracting(ColumnFamily::hasGCRule)
          .isEqualTo(Boolean.FALSE);

      BackendFactory<BigTableBackendConfig> factory =
          PersistLoader.findFactoryByName(BigTableBackendFactory.NAME);
      soft.assertThat(factory).isNotNull().isInstanceOf(BigTableBackendFactory.class);
      try (Backend backend =
          factory.buildBackend(
              BigTableBackendConfig.builder()
                  .dataClient(dataClient)
                  .tableAdminClient(tableAdminClient)
                  .build())) {
        soft.assertThat(backend).isNotNull().isInstanceOf(BigTableBackend.class);
        backend.setupSchema();
      }

      soft.assertThat(
              tableAdminClient.getTable(TABLE_REFS).getColumnFamilies().stream()
                  .filter(cf -> cf.getId().equals(FAMILY_REFS))
                  .findFirst())
          .get()
          .extracting(ColumnFamily::hasGCRule, ColumnFamily::getGCRule)
          .containsExactly(Boolean.TRUE, GCRULE_MAX_VERSIONS_1);
      soft.assertThat(
              tableAdminClient.getTable(TABLE_OBJS).getColumnFamilies().stream()
                  .filter(cf -> cf.getId().equals(FAMILY_OBJS))
                  .findFirst())
          .get()
          .extracting(ColumnFamily::hasGCRule, ColumnFamily::getGCRule)
          .containsExactly(Boolean.TRUE, GCRULE_MAX_VERSIONS_1);
    }
  }

  @Test
  public void productionLike() throws Exception {
    BackendFactory<BigTableBackendConfig> factory =
        PersistLoader.findFactoryByName(BigTableBackendFactory.NAME);
    soft.assertThat(factory).isNotNull().isInstanceOf(BigTableBackendFactory.class);

    try (BigtableDataClient dataClient = testFactory.buildNewDataClient();
        BigtableTableAdminClient tableAdminClient = testFactory.buildNewTableAdminClient()) {
      RepositoryDescription repoDesc;
      try (Backend backend =
          factory.buildBackend(
              BigTableBackendConfig.builder()
                  .dataClient(dataClient)
                  .tableAdminClient(tableAdminClient)
                  .build())) {
        soft.assertThat(backend).isNotNull().isInstanceOf(BigTableBackend.class);
        backend.setupSchema();
        PersistFactory persistFactory = backend.createFactory();
        soft.assertThat(persistFactory).isNotNull().isInstanceOf(BigTablePersistFactory.class);
        Persist persist = persistFactory.newPersist(DEFAULT_CONFIG);
        soft.assertThat(persist).isNotNull().isInstanceOf(BigTablePersist.class);

        RepositoryLogic repositoryLogic = repositoryLogic(persist);
        repositoryLogic.initialize("initializeAgain");
        repoDesc = repositoryLogic.fetchRepositoryDescription();
        soft.assertThat(repoDesc).isNotNull();
      }

      soft.assertThat(
              tableAdminClient.getTable(TABLE_REFS).getColumnFamilies().stream()
                  .filter(cf -> cf.getId().equals(FAMILY_REFS))
                  .findFirst())
          .get()
          .extracting(ColumnFamily::hasGCRule, ColumnFamily::getGCRule)
          .containsExactly(Boolean.TRUE, GCRULE_MAX_VERSIONS_1);
      soft.assertThat(
              tableAdminClient.getTable(TABLE_OBJS).getColumnFamilies().stream()
                  .filter(cf -> cf.getId().equals(FAMILY_OBJS))
                  .findFirst())
          .get()
          .extracting(ColumnFamily::hasGCRule, ColumnFamily::getGCRule)
          .containsExactly(Boolean.TRUE, GCRULE_MAX_VERSIONS_1);

      try (Backend backend =
          factory.buildBackend(
              BigTableBackendConfig.builder()
                  .dataClient(dataClient)
                  .tableAdminClient(tableAdminClient)
                  .build())) {
        soft.assertThat(backend).isNotNull().isInstanceOf(BigTableBackend.class);
        backend.setupSchema();
        PersistFactory persistFactory = backend.createFactory();
        soft.assertThat(persistFactory).isNotNull().isInstanceOf(BigTablePersistFactory.class);
        Persist persist = persistFactory.newPersist(DEFAULT_CONFIG);
        soft.assertThat(persist).isNotNull().isInstanceOf(BigTablePersist.class);

        RepositoryLogic repositoryLogic = repositoryLogic(persist);
        repositoryLogic.initialize("initializeAgain");
        soft.assertThat(repositoryLogic.fetchRepositoryDescription()).isEqualTo(repoDesc);
      }
    }
  }

  @Test
  public void backendTestFactory() throws Exception {
    BackendFactory<BigTableBackendConfig> factory =
        PersistLoader.findFactoryByName(BigTableBackendFactory.NAME);
    soft.assertThat(factory).isNotNull().isInstanceOf(BigTableBackendFactory.class);

    RepositoryDescription repoDesc;
    try (Backend backend = testFactory.createNewBackend()) {
      soft.assertThat(backend).isNotNull().isInstanceOf(BigTableBackend.class);
      backend.setupSchema();
      PersistFactory persistFactory = backend.createFactory();
      soft.assertThat(persistFactory).isNotNull().isInstanceOf(BigTablePersistFactory.class);
      Persist persist = persistFactory.newPersist(DEFAULT_CONFIG);
      soft.assertThat(persist).isNotNull().isInstanceOf(BigTablePersist.class);

      RepositoryLogic repositoryLogic = repositoryLogic(persist);
      repositoryLogic.initialize("initializeAgain");
      repoDesc = repositoryLogic.fetchRepositoryDescription();
      soft.assertThat(repoDesc).isNotNull();
    }

    try (Backend backend = testFactory.createNewBackend()) {
      soft.assertThat(backend).isNotNull().isInstanceOf(BigTableBackend.class);
      backend.setupSchema();
      PersistFactory persistFactory = backend.createFactory();
      soft.assertThat(persistFactory).isNotNull().isInstanceOf(BigTablePersistFactory.class);
      Persist persist = persistFactory.newPersist(DEFAULT_CONFIG);
      soft.assertThat(persist).isNotNull().isInstanceOf(BigTablePersist.class);

      RepositoryLogic repositoryLogic = repositoryLogic(persist);
      repositoryLogic.initialize("initializeAgain");
      soft.assertThat(repositoryLogic.fetchRepositoryDescription()).isEqualTo(repoDesc);
    }
  }
}
