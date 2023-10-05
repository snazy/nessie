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

import static com.google.cloud.bigtable.data.v2.models.Filters.FILTERS;
import static com.google.common.base.Preconditions.checkState;
import static com.google.protobuf.ByteString.copyFromUtf8;
import static java.util.Objects.requireNonNull;
import static org.projectnessie.versioned.storage.bigtable.BigTableConstants.FAMILY_OBJS;
import static org.projectnessie.versioned.storage.bigtable.BigTableConstants.FAMILY_REFS;
import static org.projectnessie.versioned.storage.bigtable.BigTableConstants.GCRULE_MAX_VERSIONS_1;
import static org.projectnessie.versioned.storage.bigtable.BigTableConstants.TABLE_OBJS;
import static org.projectnessie.versioned.storage.bigtable.BigTableConstants.TABLE_REFS;
import static org.projectnessie.versioned.storage.bigtable.BigTablePersist.apiException;

import com.google.api.gax.batching.Batcher;
import com.google.api.gax.rpc.ApiException;
import com.google.api.gax.rpc.NotFoundException;
import com.google.api.gax.rpc.ServerStream;
import com.google.cloud.bigtable.admin.v2.BigtableTableAdminClient;
import com.google.cloud.bigtable.admin.v2.models.CreateTableRequest;
import com.google.cloud.bigtable.admin.v2.models.ModifyColumnFamiliesRequest;
import com.google.cloud.bigtable.data.v2.BigtableDataClient;
import com.google.cloud.bigtable.data.v2.models.Filters;
import com.google.cloud.bigtable.data.v2.models.Filters.Filter;
import com.google.cloud.bigtable.data.v2.models.Query;
import com.google.cloud.bigtable.data.v2.models.Row;
import com.google.cloud.bigtable.data.v2.models.RowMutationEntry;
import com.google.protobuf.ByteString;
import java.util.ArrayDeque;
import java.util.Collection;
import java.util.List;
import java.util.Queue;
import java.util.Set;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import org.projectnessie.versioned.storage.common.persist.Backend;
import org.projectnessie.versioned.storage.common.persist.PersistFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

final class BigTableBackend implements Backend {
  private static final Logger LOGGER = LoggerFactory.getLogger(BigTableBackend.class);
  static final ByteString REPO_REGEX_SUFFIX = copyFromUtf8("\\C*");

  private final BigtableDataClient dataClient;
  private final BigtableTableAdminClient tableAdminClient;
  private final boolean closeClient;

  final String tableRefs;
  final String tableObjs;

  BigTableBackend(
      @Nonnull @jakarta.annotation.Nonnull BigTableBackendConfig config, boolean closeClient) {
    this.dataClient = config.dataClient();
    this.tableAdminClient = config.tableAdminClient();
    this.tableRefs =
        config.tablePrefix().map(prefix -> prefix + '_' + TABLE_REFS).orElse(TABLE_REFS);
    this.tableObjs =
        config.tablePrefix().map(prefix -> prefix + '_' + TABLE_OBJS).orElse(TABLE_OBJS);
    this.closeClient = closeClient;
  }

  @Nonnull
  @jakarta.annotation.Nonnull
  BigtableDataClient client() {
    return dataClient;
  }

  @Nullable
  @jakarta.annotation.Nullable
  BigtableTableAdminClient adminClient() {
    return tableAdminClient;
  }

  @Override
  @Nonnull
  @jakarta.annotation.Nonnull
  public PersistFactory createFactory() {
    return new BigTablePersistFactory(this);
  }

  @Override
  public void close() {
    if (closeClient) {
      RuntimeException ex = null;
      try {
        dataClient.close();
      } catch (Exception e) {
        ex = new RuntimeException(e);
      }
      try {
        if (tableAdminClient != null) {
          tableAdminClient.close();
        }
      } catch (Exception e) {
        if (ex == null) {
          ex = new RuntimeException(e);
        } else {
          ex.addSuppressed(e);
        }
      }
      if (ex != null) {
        throw ex;
      }
    }
  }

  @Override
  public void setupSchema() {
    if (tableAdminClient == null) {
      // If BigTable admin client is not available, check at least that the required tables exist.
      boolean refs = checkTableNoAdmin(tableRefs);
      boolean objs = checkTableNoAdmin(tableObjs);
      checkState(
          refs && objs,
          "Not all required tables (%s and %s) are available in BigTable, cannot start.",
          tableRefs,
          tableObjs);
      LOGGER.info("No Bigtable admin client available, skipping schema setup");
      return;
    }

    checkTable(tableRefs, FAMILY_REFS);
    checkTable(tableObjs, FAMILY_OBJS);
  }

  private boolean checkTableNoAdmin(String table) {
    try {
      dataClient.readRow(table, "dummy");
      return true;
    } catch (NotFoundException nf) {
      LOGGER.error("Nessie table '{}' does not exist in Google Bigtable", table);
    }
    return false;
  }

  private void checkTable(String table, String family) {
    BigtableTableAdminClient client = requireNonNull(tableAdminClient);
    try {
      client.getTable(table).getColumnFamilies().stream()
          .filter(cf -> cf.getId().equals(family))
          .filter(cf -> !cf.hasGCRule())
          .forEach(
              cf ->
                  client.modifyFamilies(
                      ModifyColumnFamiliesRequest.of(table)
                          .updateFamily(family, GCRULE_MAX_VERSIONS_1)));
    } catch (NotFoundException nf) {
      LOGGER.info("Creating Nessie table '{}' in Google Bigtable...", table);
      client.createTable(CreateTableRequest.of(table).addFamily(family, GCRULE_MAX_VERSIONS_1));
    }
  }

  @Override
  public void eraseRepositories(Set<String> repositoryIds) {
    Queue<String> toDelete = new ArrayDeque<>(repositoryIds);
    eraseRepositoriesAdminClient(toDelete);
    eraseRepositoriesNoAdminClient(toDelete);
  }

  private void eraseRepositoriesAdminClient(Queue<String> repositoryIds) {
    if (tableAdminClient != null) {
      while (!repositoryIds.isEmpty()) {
        String repoId = repositoryIds.peek();
        ByteString prefix = copyFromUtf8(repoId + ':');
        try {
          tableAdminClient.dropRowRange(tableRefs, prefix);
          tableAdminClient.dropRowRange(tableObjs, prefix);
        } catch (ApiException e) {
          LOGGER.warn("Could not erase repo with admin client, switching to data client", e);
          return;
        }
        repositoryIds.poll();
      }
    }
  }

  private void eraseRepositoriesNoAdminClient(Collection<String> repositoryIds) {
    if (!repositoryIds.isEmpty()) {
      List<ByteString> prefixes =
          repositoryIds.stream()
              .map(repoId -> copyFromUtf8(repoId + ':'))
              .collect(Collectors.toList());
      eraseRepositoriesTable(tableRefs, prefixes);
      eraseRepositoriesTable(tableObjs, prefixes);
    }
  }

  private void eraseRepositoriesTable(String tableId, List<ByteString> prefixes) {

    Query query =
        Query.create(tableId)
            .filter(FILTERS.chain().filter(repoFilter(prefixes)).filter(FILTERS.value().strip()));

    try (Batcher<RowMutationEntry, Void> batcher = dataClient.newBulkMutationBatcher(tableId)) {
      ServerStream<Row> rows = dataClient.readRows(query);
      for (Row row : rows) {
        batcher.add(RowMutationEntry.create(row.getKey()).deleteRow());
      }
    } catch (ApiException e) {
      throw apiException(e);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new RuntimeException(e);
    }
  }

  private static Filter repoFilter(List<ByteString> prefixes) {
    if (prefixes.size() == 1) {
      return FILTERS.key().regex(prefixes.get(0).concat(REPO_REGEX_SUFFIX));
    } else {
      Filters.InterleaveFilter filter = FILTERS.interleave();
      for (ByteString prefix : prefixes) {
        filter.filter(FILTERS.key().regex(prefix.concat(REPO_REGEX_SUFFIX)));
      }
      return filter;
    }
  }

  @Override
  public String configInfo() {
    return this.tableAdminClient != null ? "" : " (no admin client)";
  }
}
