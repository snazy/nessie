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
package org.projectnessie.versioned.storage.transitional;

import static com.google.common.base.Preconditions.checkState;
import static java.util.Spliterators.spliteratorUnknownSize;
import static java.util.stream.StreamSupport.stream;
import static org.projectnessie.versioned.storage.common.logic.Logics.referenceLogic;
import static org.projectnessie.versioned.storage.common.logic.ReferencesQuery.referencesQuery;
import static org.projectnessie.versioned.storage.transitional.TransitionalPersist.TransitionalStatus.EVENTUAL;
import static org.projectnessie.versioned.storage.transitional.TransitionalPersist.TransitionalStatus.FAILED;
import static org.projectnessie.versioned.storage.transitional.TransitionalPersist.TransitionalStatus.INTERMEDIATE;
import static org.projectnessie.versioned.storage.transitional.TransitionalPersist.TransitionalStatus.TRANSITIONING;

import jakarta.annotation.Nullable;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.ForkJoinTask;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.IntConsumer;
import java.util.function.ToIntFunction;
import javax.annotation.Nonnull;
import org.projectnessie.versioned.storage.common.config.StoreConfig;
import org.projectnessie.versioned.storage.common.exceptions.ObjNotFoundException;
import org.projectnessie.versioned.storage.common.exceptions.ObjTooLargeException;
import org.projectnessie.versioned.storage.common.exceptions.RefAlreadyExistsException;
import org.projectnessie.versioned.storage.common.exceptions.RefConditionFailedException;
import org.projectnessie.versioned.storage.common.exceptions.RefNotFoundException;
import org.projectnessie.versioned.storage.common.exceptions.RetryTimeoutException;
import org.projectnessie.versioned.storage.common.logic.PagedResult;
import org.projectnessie.versioned.storage.common.logic.ReferenceLogic;
import org.projectnessie.versioned.storage.common.persist.CloseableIterator;
import org.projectnessie.versioned.storage.common.persist.Obj;
import org.projectnessie.versioned.storage.common.persist.ObjId;
import org.projectnessie.versioned.storage.common.persist.ObjType;
import org.projectnessie.versioned.storage.common.persist.Persist;
import org.projectnessie.versioned.storage.common.persist.Reference;

class TransitionalPersistImpl implements TransitionalPersist {

  private final TransitionalConfig backend;
  private final StoreConfig config;
  private final AtomicReference<TransitionalStatus> status;

  TransitionalPersistImpl(TransitionalConfig backend, StoreConfig config) {
    this.backend = backend;
    this.config = config;
    this.status = new AtomicReference<>(INTERMEDIATE);
  }

  @Nonnull
  @jakarta.annotation.Nonnull
  @Override
  public String name() {
    return "Transitional(" + intermediate().name() + "," + eventual().name() + ")";
  }

  @Override
  @Nonnull
  @jakarta.annotation.Nonnull
  public StoreConfig config() {
    return config;
  }

  @Override
  public TransitionalStatus status() {
    return status.get();
  }

  private Persist intermediate() {
    return backend.intermediatePersist();
  }

  private Persist eventual() {
    return backend.intermediatePersist();
  }

  private Persist current() {
    switch (status()) {
      case INTERMEDIATE:
        return intermediate();
      case EVENTUAL:
        return eventual();
      case TRANSITIONING:
        throw new IllegalStateException(
            "Must not use a transitional Persist while it is transitioning");
      case FAILED:
        throw new IllegalStateException(
            "A previous failure, usually during the transition operation, invalidated the transitional Persist instance");
      default:
        throw new UnsupportedOperationException();
    }
  }

  @Override
  public void transition(
      int objStoreBatchSize,
      int createReferencesParallelism,
      IntConsumer commitsTransitioned,
      Runnable referenceCreated) {
    checkState(
        status.compareAndSet(INTERMEDIATE, TRANSITIONING),
        "Transitional Persist not in expected state INTERMEDIATE but in %s",
        status.get());

    boolean success = false;
    try {
      Persist intermediate = intermediate();
      Persist eventual = eventual();

      List<Obj> batchStore = new ArrayList<>();

      try (CloseableIterator<Obj> objects =
          intermediate.scanAllObjects(EnumSet.allOf(ObjType.class))) {
        while (objects.hasNext()) {
          Obj obj = objects.next();
          batchStore.add(obj);
          if (batchStore.size() >= objStoreBatchSize) {
            eventual.storeObjs(batchStore.toArray(new Obj[0]));
            commitsTransitioned.accept(batchStore.size());
            batchStore.clear();
          }
        }
        if (!batchStore.isEmpty()) {
          eventual.storeObjs(batchStore.toArray(new Obj[0]));
          commitsTransitioned.accept(batchStore.size());
          batchStore.clear();
        }
      } catch (ObjTooLargeException e) {
        throw new RuntimeException(e);
      }

      ReferenceLogic intermediateReferences = referenceLogic(intermediate);
      ReferenceLogic eventualReferences = referenceLogic(eventual);
      PagedResult<Reference, String> references =
          intermediateReferences.queryReferences(referencesQuery());

      // Create references in parallel (uses the machine's fork-join-pool)
      ForkJoinPool forkJoinPool = new ForkJoinPool(createReferencesParallelism);
      try {
        forkJoinPool.invoke(
            ForkJoinTask.adapt(
                () ->
                    stream(spliteratorUnknownSize(references, 0), true)
                        .parallel()
                        .forEach(
                            ref -> {
                              try {
                                eventualReferences.createReference(ref.name(), ref.pointer());
                                referenceCreated.run();
                              } catch (RefAlreadyExistsException | RetryTimeoutException e) {
                                throw new RuntimeException(e);
                              }
                            })));
      } finally {
        forkJoinPool.shutdown();
      }

      checkState(
          status.compareAndSet(TRANSITIONING, EVENTUAL),
          "Transitional Persist not in expected state TRANSITIONING but in %s",
          status.get());
      success = true;
    } finally {
      if (!success) {
        status.set(FAILED);
      }
    }
  }

  private int minimum(ToIntFunction<Persist> supplier) {
    return Math.min(supplier.applyAsInt(intermediate()), supplier.applyAsInt(eventual()));
  }

  @Override
  public int hardObjectSizeLimit() {
    return minimum(Persist::hardObjectSizeLimit);
  }

  @Override
  public int effectiveIndexSegmentSizeLimit() {
    return minimum(Persist::effectiveIndexSegmentSizeLimit);
  }

  @Override
  public int effectiveIncrementalIndexSizeLimit() {
    return minimum(Persist::effectiveIncrementalIndexSizeLimit);
  }

  @Override
  @jakarta.annotation.Nonnull
  @Nonnull
  public Reference addReference(@Nonnull Reference reference) throws RefAlreadyExistsException {
    return current().addReference(reference);
  }

  @Override
  @jakarta.annotation.Nonnull
  @Nonnull
  public Reference markReferenceAsDeleted(@Nonnull Reference reference)
      throws RefNotFoundException, RefConditionFailedException {
    return current().markReferenceAsDeleted(reference);
  }

  @Override
  public void purgeReference(@Nonnull Reference reference)
      throws RefNotFoundException, RefConditionFailedException {
    current().purgeReference(reference);
  }

  @Override
  @jakarta.annotation.Nonnull
  @Nonnull
  public Reference updateReferencePointer(@Nonnull Reference reference, @Nonnull ObjId newPointer)
      throws RefNotFoundException, RefConditionFailedException {
    return current().updateReferencePointer(reference, newPointer);
  }

  @Override
  @Nullable
  @javax.annotation.Nullable
  public Reference fetchReference(@Nonnull String name) {
    return current().fetchReference(name);
  }

  @Override
  @jakarta.annotation.Nonnull
  @Nonnull
  public Reference[] fetchReferences(@Nonnull String[] names) {
    return current().fetchReferences(names);
  }

  @Override
  @jakarta.annotation.Nonnull
  @Nonnull
  public Obj fetchObj(@Nonnull ObjId id) throws ObjNotFoundException {
    return current().fetchObj(id);
  }

  @Override
  @jakarta.annotation.Nonnull
  @Nonnull
  public <T extends Obj> T fetchTypedObj(@Nonnull ObjId id, ObjType type, Class<T> typeClass)
      throws ObjNotFoundException {
    return current().fetchTypedObj(id, type, typeClass);
  }

  @Override
  @jakarta.annotation.Nonnull
  @Nonnull
  public ObjType fetchObjType(@Nonnull ObjId id) throws ObjNotFoundException {
    return current().fetchObjType(id);
  }

  @Override
  @jakarta.annotation.Nonnull
  @Nonnull
  public Obj[] fetchObjs(@Nonnull ObjId[] ids) throws ObjNotFoundException {
    return current().fetchObjs(ids);
  }

  @Override
  public boolean storeObj(@Nonnull Obj obj) throws ObjTooLargeException {
    return current().storeObj(obj);
  }

  @Override
  public boolean storeObj(@Nonnull Obj obj, boolean ignoreSoftSizeRestrictions)
      throws ObjTooLargeException {
    return current().storeObj(obj, ignoreSoftSizeRestrictions);
  }

  @Override
  @jakarta.annotation.Nonnull
  @Nonnull
  public boolean[] storeObjs(@Nonnull Obj[] objs) throws ObjTooLargeException {
    return current().storeObjs(objs);
  }

  @Override
  public void deleteObj(@Nonnull ObjId id) {
    current().deleteObj(id);
  }

  @Override
  public void deleteObjs(@Nonnull ObjId[] ids) {
    current().deleteObjs(ids);
  }

  @Override
  public void updateObj(@Nonnull Obj obj) throws ObjTooLargeException, ObjNotFoundException {
    current().updateObj(obj);
  }

  @Override
  public void updateObjs(@Nonnull Obj[] objs) throws ObjTooLargeException, ObjNotFoundException {
    current().updateObjs(objs);
  }

  @Override
  @jakarta.annotation.Nonnull
  @Nonnull
  public CloseableIterator<Obj> scanAllObjects(@Nonnull Set<ObjType> returnedObjTypes) {
    return current().scanAllObjects(returnedObjTypes);
  }

  @Override
  public void erase() {
    current().erase();
  }
}
