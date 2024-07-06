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
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.protobuf.ByteString.copyFromUtf8;
import static com.google.protobuf.UnsafeByteOperations.unsafeWrap;
import static java.util.Collections.singleton;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.projectnessie.versioned.storage.bigtable.BigTableConstants.CELL_TIMESTAMP;
import static org.projectnessie.versioned.storage.bigtable.BigTableConstants.DEFAULT_BULK_READ_TIMEOUT;
import static org.projectnessie.versioned.storage.bigtable.BigTableConstants.FAMILY_OBJS;
import static org.projectnessie.versioned.storage.bigtable.BigTableConstants.FAMILY_REFS;
import static org.projectnessie.versioned.storage.bigtable.BigTableConstants.MAX_PARALLEL_READS;
import static org.projectnessie.versioned.storage.bigtable.BigTableConstants.QUALIFIER_OBJS;
import static org.projectnessie.versioned.storage.bigtable.BigTableConstants.QUALIFIER_OBJS_OR_VERS;
import static org.projectnessie.versioned.storage.bigtable.BigTableConstants.QUALIFIER_OBJ_TYPE;
import static org.projectnessie.versioned.storage.bigtable.BigTableConstants.QUALIFIER_OBJ_VERS;
import static org.projectnessie.versioned.storage.bigtable.BigTableConstants.QUALIFIER_REFS;
import static org.projectnessie.versioned.storage.common.persist.ObjId.objIdFromByteBuffer;
import static org.projectnessie.versioned.storage.serialize.ProtoSerialization.deserializeObj;
import static org.projectnessie.versioned.storage.serialize.ProtoSerialization.deserializeReference;
import static org.projectnessie.versioned.storage.serialize.ProtoSerialization.serializeObj;
import static org.projectnessie.versioned.storage.serialize.ProtoSerialization.serializeReference;

import com.google.api.core.ApiFuture;
import com.google.api.gax.batching.Batcher;
import com.google.api.gax.rpc.AbortedException;
import com.google.api.gax.rpc.ApiException;
import com.google.api.gax.rpc.DeadlineExceededException;
import com.google.api.gax.rpc.UnknownException;
import com.google.api.gax.rpc.WatchdogTimeoutException;
import com.google.cloud.bigtable.data.v2.models.ConditionalRowMutation;
import com.google.cloud.bigtable.data.v2.models.Filters;
import com.google.cloud.bigtable.data.v2.models.Filters.Filter;
import com.google.cloud.bigtable.data.v2.models.Mutation;
import com.google.cloud.bigtable.data.v2.models.MutationApi;
import com.google.cloud.bigtable.data.v2.models.Query;
import com.google.cloud.bigtable.data.v2.models.Row;
import com.google.cloud.bigtable.data.v2.models.RowCell;
import com.google.cloud.bigtable.data.v2.models.RowMutation;
import com.google.cloud.bigtable.data.v2.models.RowMutationEntry;
import com.google.cloud.bigtable.data.v2.models.TableId;
import com.google.common.collect.AbstractIterator;
import com.google.common.collect.ImmutableMap;
import com.google.protobuf.ByteString;
import jakarta.annotation.Nonnull;
import java.lang.reflect.Array;
import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;
import org.projectnessie.versioned.storage.common.config.StoreConfig;
import org.projectnessie.versioned.storage.common.exceptions.ObjNotFoundException;
import org.projectnessie.versioned.storage.common.exceptions.ObjTooLargeException;
import org.projectnessie.versioned.storage.common.exceptions.RefAlreadyExistsException;
import org.projectnessie.versioned.storage.common.exceptions.RefConditionFailedException;
import org.projectnessie.versioned.storage.common.exceptions.RefNotFoundException;
import org.projectnessie.versioned.storage.common.exceptions.UnknownOperationResultException;
import org.projectnessie.versioned.storage.common.persist.CloseableIterator;
import org.projectnessie.versioned.storage.common.persist.Obj;
import org.projectnessie.versioned.storage.common.persist.ObjId;
import org.projectnessie.versioned.storage.common.persist.ObjType;
import org.projectnessie.versioned.storage.common.persist.ObjTypes;
import org.projectnessie.versioned.storage.common.persist.Persist;
import org.projectnessie.versioned.storage.common.persist.Reference;
import org.projectnessie.versioned.storage.common.persist.UpdateableObj;

public class BigTablePersist implements Persist {

  private final BigTableBackend backend;
  private final StoreConfig config;
  private final ByteString keyPrefix;
  private final long apiTimeoutMillis;

  BigTablePersist(BigTableBackend backend, StoreConfig config) {
    this.backend = backend;
    this.config = config;
    this.keyPrefix = copyFromUtf8(config.repositoryId() + ':');
    this.apiTimeoutMillis =
        backend.config().totalApiTimeout().orElse(DEFAULT_BULK_READ_TIMEOUT).toMillis();
  }

  static RuntimeException apiException(ApiException e) {
    if (e instanceof DeadlineExceededException
        || e instanceof WatchdogTimeoutException
        || e instanceof UnknownException
        || e instanceof AbortedException) {
      throw new UnknownOperationResultException("Unhandled BigTable exception", e);
    }
    throw new RuntimeException("Unhandled BigTable exception", e);
  }

  private ByteString dbKey(ByteString key) {
    return keyPrefix.concat(key);
  }

  private ByteString dbKey(String key) {
    return dbKey(copyFromUtf8(key));
  }

  private ByteString dbKey(ObjId id) {
    return dbKey(unsafeWrap(id.asByteArray()));
  }

  @Nonnull
  @Override
  public String name() {
    return BigTableBackendFactory.NAME;
  }

  @Override
  @Nonnull
  public StoreConfig config() {
    return config;
  }

  @Override
  public Reference fetchReference(@Nonnull String name) {
    try {
      ByteString key = dbKey(name);
      Row row = backend.client().readRow(backend.tableRefsId, key);
      return row != null ? referenceFromRow(row) : null;
    } catch (ApiException e) {
      throw apiException(e);
    }
  }

  @Override
  @Nonnull
  public Reference[] fetchReferences(@Nonnull String[] names) {
    try {
      Reference[] r = new Reference[names.length];
      bulkFetch(
          backend.tableRefsId,
          names,
          r,
          this::dbKey,
          BigTablePersist::referenceFromRow,
          name -> {});
      return r;
    } catch (ExecutionException | TimeoutException e) {
      throw new RuntimeException(e);
    } catch (ApiException e) {
      throw apiException(e);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new RuntimeException(e);
    }
  }

  @Override
  @Nonnull
  public Reference addReference(@Nonnull Reference reference) throws RefAlreadyExistsException {
    checkArgument(!reference.deleted(), "Deleted references must not be added");

    try {
      ByteString key = dbKey(reference.name());

      Mutation mutation = refsMutation(reference);
      Filter condition =
          FILTERS
              .chain()
              .filter(FILTERS.family().exactMatch(FAMILY_REFS))
              .filter(FILTERS.qualifier().exactMatch(QUALIFIER_REFS));

      boolean failure =
          backend
              .client()
              .checkAndMutateRow(
                  ConditionalRowMutation.create(backend.tableRefsId, key)
                      .condition(condition)
                      .otherwise(mutation));

      if (failure) {
        throw new RefAlreadyExistsException(fetchReference(reference.name()));
      }

      return reference;
    } catch (ApiException e) {
      throw apiException(e);
    }
  }

  @Override
  @Nonnull
  public Reference markReferenceAsDeleted(@Nonnull Reference reference)
      throws RefNotFoundException, RefConditionFailedException {
    try {
      ByteString key = dbKey(reference.name());

      Reference expected = reference.withDeleted(false);
      Reference deleted = reference.withDeleted(true);

      casReferenceAndThrow(reference, key, expected, refsMutation(deleted));
      return deleted;
    } catch (ApiException e) {
      throw apiException(e);
    }
  }

  @Override
  @Nonnull
  public Reference updateReferencePointer(@Nonnull Reference reference, @Nonnull ObjId newPointer)
      throws RefNotFoundException, RefConditionFailedException {
    try {
      ByteString key = dbKey(reference.name());

      Reference expected = reference.withDeleted(false);
      Reference updated = reference.forNewPointer(newPointer, config);

      casReferenceAndThrow(reference, key, expected, refsMutation(updated));
      return updated;
    } catch (ApiException e) {
      throw apiException(e);
    }
  }

  @Override
  public void purgeReference(@Nonnull Reference reference)
      throws RefNotFoundException, RefConditionFailedException {
    try {
      ByteString key = dbKey(reference.name());

      Reference expected = reference.withDeleted(true);

      casReferenceAndThrow(reference, key, expected, Mutation.create().deleteRow());
    } catch (ApiException e) {
      throw apiException(e);
    }
  }

  @Nonnull
  private static Mutation refsMutation(@Nonnull Reference reference) {
    return Mutation.create()
        .setCell(
            FAMILY_REFS,
            QUALIFIER_REFS,
            // Note: must use a constant timestamp, otherwise BigTable will pile up historic values,
            // which would also break our CAS conditions, because historic values could match.
            CELL_TIMESTAMP,
            unsafeWrap(serializeReference(reference)));
  }

  @Nonnull
  private static Filters.ChainFilter refsValueFilter(Reference expected) {
    return FILTERS
        .chain()
        .filter(FILTERS.family().exactMatch(FAMILY_REFS))
        .filter(FILTERS.qualifier().exactMatch(QUALIFIER_REFS))
        .filter(FILTERS.value().exactMatch(unsafeWrap(serializeReference(expected))));
  }

  private void casReferenceAndThrow(
      @Nonnull Reference reference, ByteString key, Reference expected, Mutation mutation)
      throws RefConditionFailedException, RefNotFoundException {
    if (!casReference(key, refsValueFilter(expected), mutation)) {
      Reference r = fetchReference(reference.name());
      if (r != null) {
        throw new RefConditionFailedException(r);
      } else {
        throw new RefNotFoundException(reference);
      }
    }
  }

  private Boolean casReference(ByteString key, Filter condition, Mutation mutation) {
    return backend
        .client()
        .checkAndMutateRow(
            ConditionalRowMutation.create(backend.tableRefsId, key)
                .condition(condition)
                .then(mutation));
  }

  private static Reference referenceFromRow(Row row) {
    List<RowCell> cells = row.getCells(FAMILY_REFS);
    for (RowCell cell : cells) {
      if (cell.getQualifier().equals(QUALIFIER_REFS)) {
        return deserializeReference(cell.getValue().toByteArray());
      }
    }
    throw new IllegalStateException("Row has no CF " + FAMILY_REFS);
  }

  @Override
  @Nonnull
  public <T extends Obj> T fetchTypedObj(
      @Nonnull ObjId id, ObjType type, @Nonnull Class<T> typeClass) throws ObjNotFoundException {
    try {
      ByteString key = dbKey(id);

      Row row = backend.client().readRow(backend.tableObjsId, key);
      if (row != null) {
        Obj obj = objFromRow(row);
        if (type != null && !type.equals(obj.type())) {
          throw new ObjNotFoundException(id);
        }
        @SuppressWarnings("unchecked")
        T r = (T) obj;
        return r;
      }
      throw new ObjNotFoundException(id);
    } catch (ApiException e) {
      throw apiException(e);
    }
  }

  @Override
  public <T extends Obj> T[] fetchTypedObjsIfExist(
      @Nonnull ObjId[] ids, ObjType type, @Nonnull Class<T> typeClass) {
    try {
      @SuppressWarnings("unchecked")
      T[] r = (T[]) Array.newInstance(typeClass, ids.length);

      bulkFetch(backend.tableObjsId, ids, r, this::dbKey, this::objFromRow, x -> {});

      if (type != null) {
        for (int i = 0; i < r.length; i++) {
          Obj o = r[i];
          if (o != null && !type.equals(o.type())) {
            r[i] = null;
          }
        }
      }

      return r;
    } catch (ExecutionException | TimeoutException e) {
      throw new RuntimeException(e);
    } catch (ApiException e) {
      throw apiException(e);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new RuntimeException(e);
    }
  }

  @Override
  public boolean storeObj(@Nonnull Obj obj, boolean ignoreSoftSizeRestrictions)
      throws ObjTooLargeException {

    try {
      ConditionalRowMutation conditionalRowMutation =
          mutationForStoreObj(obj, ignoreSoftSizeRestrictions);

      boolean success = backend.client().checkAndMutateRow(conditionalRowMutation);
      return !success;
    } catch (ApiException e) {
      throw apiException(e);
    }
  }

  private static final Map<ObjType, ByteString> OBJ_TYPE_VALUES =
      ObjTypes.allObjTypes().stream()
          .collect(
              ImmutableMap.toImmutableMap(
                  Function.identity(), (ObjType type) -> ByteString.copyFromUtf8(type.name())));

  @Nonnull
  private ConditionalRowMutation mutationForStoreObj(
      @Nonnull Obj obj, boolean ignoreSoftSizeRestrictions) throws ObjTooLargeException {
    checkArgument(obj.id() != null, "Obj to store must have a non-null ID");
    ByteString key = dbKey(obj.id());

    Mutation mutation = objectWriteMutation(obj, ignoreSoftSizeRestrictions);

    Filter condition =
        FILTERS
            .chain()
            .filter(FILTERS.key().exactMatch(key))
            .filter(FILTERS.family().exactMatch(FAMILY_OBJS))
            .filter(FILTERS.qualifier().exactMatch(QUALIFIER_OBJS));
    return ConditionalRowMutation.create(backend.tableObjsId, key)
        .condition(condition)
        .otherwise(mutation);
  }

  @Override
  @Nonnull
  public boolean[] storeObjs(@Nonnull Obj[] objs) throws ObjTooLargeException {
    if (objs.length == 0) {
      return new boolean[0];
    }

    if (objs.length == 1) {
      Obj obj = objs[0];
      boolean r = obj != null && storeObj(obj);
      return new boolean[] {r};
    }

    @SuppressWarnings("unchecked")
    ApiFuture<Boolean>[] futures = new ApiFuture[objs.length];
    for (int i = 0; i < objs.length; i++) {
      Obj obj = objs[i];
      if (obj != null) {
        ConditionalRowMutation conditionalRowMutation = mutationForStoreObj(obj, false);
        futures[i] = backend.client().checkAndMutateRowAsync(conditionalRowMutation);
      }
    }

    boolean[] r = new boolean[objs.length];
    for (int i = 0; i < objs.length; i++) {
      Obj o = objs[i];
      if (o != null) {
        try {
          r[i] = !futures[i].get();
        } catch (Exception e) {
          throw new RuntimeException(e);
        }
      }
    }
    return r;
  }

  @Override
  public void deleteObj(@Nonnull ObjId id) {
    try {
      ByteString key = dbKey(id);
      backend
          .client()
          .mutateRow(RowMutation.create(backend.tableObjsId, key, Mutation.create().deleteRow()));
    } catch (ApiException e) {
      throw apiException(e);
    }
  }

  @Override
  public void deleteObjs(@Nonnull ObjId[] ids) {
    if (ids.length == 0) {
      return;
    }
    try (Batcher<RowMutationEntry, Void> batcher =
        backend.client().newBulkMutationBatcher(backend.tableObjsId)) {
      for (ObjId id : ids) {
        if (id != null) {
          ByteString key = dbKey(id);
          batcher.add(RowMutationEntry.create(key).deleteRow());
        }
      }
    } catch (ApiException e) {
      throw apiException(e);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new RuntimeException(e);
    }
  }

  @Override
  public void upsertObj(@Nonnull Obj obj) throws ObjTooLargeException {
    ObjId id = obj.id();
    checkArgument(id != null, "Obj to store must have a non-null ID");

    try {
      ByteString key = dbKey(id);

      byte[] serialized =
          serializeObj(
              obj, effectiveIncrementalIndexSizeLimit(), effectiveIndexSegmentSizeLimit(), false);

      backend
          .client()
          .mutateRow(objToMutation(obj, RowMutation.create(backend.tableObjsId, key), serialized));
    } catch (ApiException e) {
      throw apiException(e);
    }
  }

  @Override
  public void upsertObjs(@Nonnull Obj[] objs) throws ObjTooLargeException {
    if (objs.length == 0) {
      return;
    }

    try (Batcher<RowMutationEntry, Void> batcher =
        backend.client().newBulkMutationBatcher(backend.tableObjsId)) {
      for (Obj obj : objs) {
        if (obj == null) {
          continue;
        }
        ObjId id = obj.id();
        checkArgument(id != null, "Obj to store must have a non-null ID");

        ByteString key = dbKey(id);

        byte[] serialized =
            serializeObj(
                obj, effectiveIncrementalIndexSizeLimit(), effectiveIndexSegmentSizeLimit(), false);

        batcher.add(objToMutation(obj, RowMutationEntry.create(key), serialized));
      }
    } catch (ApiException e) {
      throw apiException(e);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new RuntimeException(e);
    }
  }

  @Override
  public boolean deleteConditional(@Nonnull UpdateableObj obj) {
    ConditionalRowMutation conditionalRowMutation =
        mutationForConditional(obj, Mutation.create().deleteRow());
    return backend.client().checkAndMutateRow(conditionalRowMutation);
  }

  @Override
  public boolean updateConditional(@Nonnull UpdateableObj expected, @Nonnull UpdateableObj newValue)
      throws ObjTooLargeException {
    ObjId id = expected.id();
    checkArgument(id != null && id.equals(newValue.id()));
    checkArgument(expected.type().equals(newValue.type()));
    checkArgument(!expected.versionToken().equals(newValue.versionToken()));

    Mutation mutation = objectWriteMutation(newValue, false);

    ConditionalRowMutation conditionalRowMutation = mutationForConditional(expected, mutation);
    return backend.client().checkAndMutateRow(conditionalRowMutation);
  }

  @Nonnull
  private ConditionalRowMutation mutationForConditional(
      @Nonnull UpdateableObj obj, Mutation mutation) {
    checkArgument(obj.id() != null, "Obj to store must have a non-null ID");
    ByteString key = dbKey(obj.id());

    Filters.ChainFilter objTypeFilter =
        FILTERS
            .chain()
            .filter(FILTERS.qualifier().exactMatch(QUALIFIER_OBJ_TYPE))
            .filter(FILTERS.value().exactMatch(OBJ_TYPE_VALUES.get(obj.type())));
    Filters.ChainFilter objVersionFilter =
        FILTERS
            .chain()
            .filter(FILTERS.qualifier().exactMatch(QUALIFIER_OBJ_VERS))
            .filter(FILTERS.value().exactMatch(obj.versionToken()));
    Filter condition =
        FILTERS.condition(objTypeFilter).then(objVersionFilter).otherwise(FILTERS.block());

    return ConditionalRowMutation.create(backend.tableObjsId, key)
        .condition(condition)
        .then(mutation);
  }

  @Nonnull
  private Mutation objectWriteMutation(@Nonnull Obj obj, boolean ignoreSoftSizeRestrictions)
      throws ObjTooLargeException {
    int incrementalIndexSizeLimit =
        ignoreSoftSizeRestrictions ? Integer.MAX_VALUE : effectiveIncrementalIndexSizeLimit();
    int indexSizeLimit =
        ignoreSoftSizeRestrictions ? Integer.MAX_VALUE : effectiveIndexSegmentSizeLimit();
    byte[] serialized = serializeObj(obj, incrementalIndexSizeLimit, indexSizeLimit, false);

    return objToMutation(obj, Mutation.create(), serialized);
  }

  static <M extends MutationApi<M>> M objToMutation(Obj obj, M mutation, byte[] serialized) {
    mutation
        .setCell(FAMILY_OBJS, QUALIFIER_OBJS, CELL_TIMESTAMP, unsafeWrap(serialized))
        .setCell(FAMILY_OBJS, QUALIFIER_OBJ_TYPE, CELL_TIMESTAMP, OBJ_TYPE_VALUES.get(obj.type()));
    if (obj instanceof UpdateableObj) {
      mutation.setCell(
          FAMILY_OBJS,
          QUALIFIER_OBJ_VERS,
          CELL_TIMESTAMP,
          ByteString.copyFromUtf8(((UpdateableObj) obj).versionToken()));
    }
    return mutation;
  }

  @Override
  public void erase() {
    backend.eraseRepositories(singleton(config().repositoryId()));
  }

  @Nonnull
  @Override
  public CloseableIterator<Obj> scanAllObjects(@Nonnull Set<ObjType> returnedObjTypes) {
    return new ScanAllObjectsIterator(
        returnedObjTypes.isEmpty() ? null : returnedObjTypes::contains);
  }

  private class ScanAllObjectsIterator extends AbstractIterator<Obj>
      implements CloseableIterator<Obj> {

    private final Query.QueryPaginator paginator;
    private Iterator<Row> iter;

    private ByteString lastKey;

    ScanAllObjectsIterator(Predicate<ObjType> filter) {

      Query q = Query.create(backend.tableObjsId).prefix(keyPrefix);

      Filters.ChainFilter filterChain =
          FILTERS.chain().filter(FILTERS.family().exactMatch(FAMILY_OBJS));

      Filters.InterleaveFilter typeFilter = null;
      boolean all = true;
      if (filter != null) {
        for (ObjType type : ObjTypes.allObjTypes()) {
          boolean match = filter.test(type);
          if (match) {
            if (typeFilter == null) {
              typeFilter = FILTERS.interleave();
            }
            typeFilter.filter(
                FILTERS
                    .chain()
                    .filter(FILTERS.qualifier().exactMatch(QUALIFIER_OBJ_TYPE))
                    .filter(FILTERS.value().exactMatch(OBJ_TYPE_VALUES.get(type))));
          } else {
            all = false;
          }
        }
      }
      if (!all) {
        if (typeFilter == null) {
          throw new IllegalArgumentException("No object types matched the provided predicate");
        }
        // Condition filters are generally not recommended because they are slower, but
        // scanAllObjects is not meant to be particularly efficient. The fact that we are also
        // limiting the query to a row prefix should alleviate the performance impact.
        filterChain.filter(
            FILTERS.condition(typeFilter).then(FILTERS.pass()).otherwise(FILTERS.block()));
      }

      filterChain.filter(FILTERS.qualifier().regex(QUALIFIER_OBJS_OR_VERS));

      this.paginator = q.filter(filterChain).createPaginator(100);
      this.iter = backend.client().readRows(paginator.getNextQuery()).iterator();
    }

    @Override
    public void close() {}

    @Override
    protected Obj computeNext() {
      while (true) {
        if (!iter.hasNext()) {
          if (lastKey == null) {
            return endOfData();
          }
          paginator.advance(lastKey);
          iter = backend.client().readRows(paginator.getNextQuery()).iterator();
          lastKey = null;
          continue;
        }

        Row row = iter.next();
        lastKey = row.getKey();
        return objFromRow(row);
      }
    }
  }

  private Obj objFromRow(Row row) {
    ByteString key = row.getKey().substring(keyPrefix.size());
    ObjId id = objIdFromByteBuffer(key.asReadOnlyByteBuffer());
    List<RowCell> objCells = row.getCells(FAMILY_OBJS, QUALIFIER_OBJS);
    ByteBuffer obj = objCells.get(0).getValue().asReadOnlyByteBuffer();
    List<RowCell> objVersionCells = row.getCells(FAMILY_OBJS, QUALIFIER_OBJ_VERS);
    String versionToken =
        objVersionCells.isEmpty() ? null : objVersionCells.get(0).getValue().toStringUtf8();
    return deserializeObj(id, obj, versionToken);
  }

  private <ID, R> void bulkFetch(
      TableId tableId,
      ID[] ids,
      R[] r,
      Function<ID, ByteString> keyGen,
      Function<Row, R> resultGen,
      Consumer<ID> notFound)
      throws InterruptedException, ExecutionException, TimeoutException {
    int num = ids.length;
    if (num == 0) {
      return;
    }

    ApiFuture<Row>[] handles;
    if (num <= MAX_PARALLEL_READS) {
      handles = doBulkFetch(ids, keyGen, key -> backend.client().readRowAsync(tableId, key));
    } else {
      try (Batcher<ByteString, Row> batcher = backend.client().newBulkReadRowsBatcher(tableId)) {
        handles = doBulkFetch(ids, keyGen, batcher::add);
      }
    }

    for (int idx = 0; idx < num; idx++) {
      ApiFuture<Row> handle = handles[idx];
      if (handle != null) {
        Row row = handle.get(apiTimeoutMillis, MILLISECONDS);
        if (row != null) {
          r[idx] = resultGen.apply(row);
        } else {
          notFound.accept(ids[idx]);
        }
      }
    }
  }

  private <ID> ApiFuture<Row>[] doBulkFetch(
      ID[] ids, Function<ID, ByteString> keyGen, Function<ByteString, ApiFuture<Row>> handleGen) {
    int num = ids.length;
    @SuppressWarnings("unchecked")
    ApiFuture<Row>[] handles = new ApiFuture[num];
    for (int idx = 0; idx < num; idx++) {
      ID id = ids[idx];
      if (id != null) {
        ByteString key = keyGen.apply(id);
        handles[idx] = handleGen.apply(key);
      }
    }
    return handles;
  }
}
