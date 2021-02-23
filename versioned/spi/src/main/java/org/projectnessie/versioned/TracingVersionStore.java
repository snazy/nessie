/*
 * Copyright (C) 2020 Dremio
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
package org.projectnessie.versioned;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Supplier;
import java.util.stream.Stream;

import javax.annotation.Nonnull;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.ImmutableMap;

import io.opentracing.Scope;
import io.opentracing.Tracer;
import io.opentracing.Tracer.SpanBuilder;
import io.opentracing.log.Fields;
import io.opentracing.tag.Tags;
import io.opentracing.util.GlobalTracer;

/**
 * A {@link VersionStore} wrapper that publishes tracing information via OpenTracing/OpenTelemetry.
 *
 * @param <VALUE> see {@link VersionStore}
 * @param <METADATA> see {@link VersionStore}
 */
public class TracingVersionStore<VALUE, METADATA> implements VersionStore<VALUE, METADATA> {

  private static final Logger LOGGER = LoggerFactory.getLogger(TracingVersionStore.class);

  private final VersionStore<VALUE, METADATA> delegate;

  public TracingVersionStore(VersionStore<VALUE, METADATA> delegate) {
    this.delegate = delegate;
    LOGGER.info("VersionStore emits OpenTracing information");
  }

  @Override
  @Nonnull
  public Hash toHash(@Nonnull NamedRef ref) throws ReferenceNotFoundException {
    try (Scope scope = createSpan("VersionStore.toHash")
        .withTag("nessie.version-store.operation", "ToHash")
        .withTag("nessie.version-store.ref", ref.getName())
        .startActive(true)) {
      try {
        return delegate.toHash(ref);
      } catch (RuntimeException e) {
        throw handleRuntimeException(scope, "toHash", e);
      }
    }
  }

  @Override
  public WithHash<Ref> toRef(@Nonnull String refOfUnknownType) throws ReferenceNotFoundException {
    try (Scope scope = createSpan("VersionStore.toRef")
        .withTag("nessie.version-store.operation", "ToRef")
        .withTag("nessie.version-store.ref", refOfUnknownType)
        .startActive(true)) {
      try {
        return delegate.toRef(refOfUnknownType);
      } catch (RuntimeException e) {
        throw handleRuntimeException(scope, "toRef", e);
      }
    }
  }

  @Override
  public void commit(@Nonnull BranchName branch,
      @Nonnull Optional<Hash> referenceHash,
      @Nonnull METADATA metadata,
      @Nonnull List<Operation<VALUE>> operations)
      throws ReferenceNotFoundException, ReferenceConflictException {
    try (Scope scope = createSpan("VersionStore.commit")
        .withTag("nessie.version-store.operation", "Commit")
        .withTag("nessie.version-store.branch", safeBranchName(branch))
        .withTag("nessie.version-store.hash", safeToString(referenceHash))
        .withTag("nessie.version-store.num-ops", safeSize(operations))
        .startActive(true)) {
      try {
        delegate.commit(branch, referenceHash, metadata, operations);
      } catch (RuntimeException e) {
        throw handleRuntimeException(scope, "commit", e);
      }
    }
  }

  @Override
  public void transplant(BranchName targetBranch,
      Optional<Hash> referenceHash,
      List<Hash> sequenceToTransplant)
      throws ReferenceNotFoundException, ReferenceConflictException {
    try (Scope scope = createSpan("VersionStore.transplant")
        .withTag("nessie.version-store.operation", "Transplant")
        .withTag("nessie.version-store.target-branch", safeBranchName(targetBranch))
        .withTag("nessie.version-store.hash", safeToString(referenceHash))
        .withTag("nessie.version-store.transplants", safeSize(sequenceToTransplant))
        .startActive(true)) {
      try {
        delegate.transplant(targetBranch, referenceHash, sequenceToTransplant);
      } catch (RuntimeException e) {
        throw handleRuntimeException(scope, "transplant", e);
      }
    }
  }

  @Override
  public void merge(Hash fromHash, BranchName toBranch,
      Optional<Hash> expectedHash) throws ReferenceNotFoundException, ReferenceConflictException {
    try (Scope scope = createSpan("VersionStore.merge")
        .withTag("nessie.version-store.operation", "Merge")
        .withTag("nessie.version-store.from-hash", safeToString(fromHash))
        .withTag("nessie.version-store.to-branch", safeBranchName(toBranch))
        .withTag("nessie.version-store.expected-hash", safeToString(expectedHash))
        .startActive(true)) {
      try {
        delegate.merge(fromHash, toBranch, expectedHash);
      } catch (RuntimeException e) {
        throw handleRuntimeException(scope, "merge", e);
      }
    }
  }

  @Override
  public void assign(NamedRef ref, Optional<Hash> expectedHash,
      Hash targetHash) throws ReferenceNotFoundException, ReferenceConflictException {
    try (Scope scope = createSpan("VersionStore.assign")
        .withTag("nessie.version-store.operation", "Assign")
        .withTag("nessie.version-store.ref", safeToString(ref))
        .withTag("nessie.version-store.expected-hash", safeToString(expectedHash))
        .withTag("nessie.version-store.target-hash", safeToString(targetHash))
        .startActive(true)) {
      try {
        delegate.assign(ref, expectedHash, targetHash);
      } catch (RuntimeException e) {
        throw handleRuntimeException(scope, "assign", e);
      }
    }
  }

  @Override
  public void create(NamedRef ref, Optional<Hash> targetHash)
      throws ReferenceNotFoundException, ReferenceAlreadyExistsException {
    try (Scope scope = createSpan("VersionStore.create")
        .withTag("nessie.version-store.operation", "Create")
        .withTag("nessie.version-store.ref", safeToString(ref))
        .withTag("nessie.version-store.target-hash", safeToString(targetHash))
        .startActive(true)) {
      try {
        delegate.create(ref, targetHash);
      } catch (RuntimeException e) {
        throw handleRuntimeException(scope, "create", e);
      }
    }
  }

  @Override
  public void delete(NamedRef ref, Optional<Hash> hash)
      throws ReferenceNotFoundException, ReferenceConflictException {
    try (Scope scope = createSpan("VersionStore.delete")
        .withTag("nessie.version-store.operation", "Delete")
        .withTag("nessie.version-store.ref", safeToString(ref))
        .withTag("nessie.version-store.hash", safeToString(hash))
        .startActive(true)) {
      try {
        delegate.delete(ref, hash);
      } catch (RuntimeException e) {
        throw handleRuntimeException(scope, "delete", e);
      }
    }
  }

  @Override
  public Stream<WithHash<NamedRef>> getNamedRefs() {
    Scope scope = createSpan("VersionStore.getNamedRefs")
        .withTag("nessie.version-store.operation", "GetNamedRefs")
        .startActive(true);
    try {
      return delegate.getNamedRefs().onClose(scope::close);
    } catch (RuntimeException e) {
      throw handleRuntimeExceptionClose(scope, "getNamedRefs", e);
    } finally {
      scope.span().log("getNamedRefs returns Stream");
    }
  }

  @Override
  public Stream<WithHash<METADATA>> getCommits(Ref ref) throws ReferenceNotFoundException {
    Scope scope = createSpan("VersionStore.getCommits")
        .withTag("nessie.version-store.operation", "GetCommits")
        .withTag("nessie.version-store.ref", safeToString(ref))
        .startActive(true);
    try {
      return delegate.getCommits(ref).onClose(scope::close);
    } catch (RuntimeException e) {
      throw handleRuntimeExceptionClose(scope, "getCommits", e);
    } finally {
      scope.span().log("getCommits returns Stream");
    }
  }

  @Override
  public Stream<Key> getKeys(Ref ref) throws ReferenceNotFoundException {
    Scope scope = createSpan("VersionStore.getKeys")
        .withTag("nessie.version-store.operation", "GetKeys")
        .withTag("nessie.version-store.ref", safeToString(ref))
        .startActive(true);
    try {
      return delegate.getKeys(ref).onClose(scope::close);
    } catch (RuntimeException e) {
      throw handleRuntimeExceptionClose(scope, "getKeys", e);
    } finally {
      scope.span().log("getKeys returns Stream");
    }
  }

  @Override
  public VALUE getValue(Ref ref, Key key) throws ReferenceNotFoundException {
    try (Scope scope = createSpan("VersionStore.getValue")
        .withTag("nessie.version-store.operation", "GetValue")
        .withTag("nessie.version-store.ref", safeToString(ref))
        .withTag("nessie.version-store.key", safeToString(key))
        .startActive(true)) {
      try {
        return delegate.getValue(ref, key);
      } catch (RuntimeException e) {
        throw handleRuntimeException(scope, "getValue", e);
      }
    }
  }

  @Override
  public List<Optional<VALUE>> getValues(Ref ref,
      List<Key> keys) throws ReferenceNotFoundException {
    try (Scope scope = createSpan("VersionStore.getValues")
        .withTag("nessie.version-store.operation", "GetValues")
        .withTag("nessie.version-store.ref", safeToString(ref))
        .withTag("nessie.version-store.keys", safeToString(keys))
        .startActive(true)) {
      try {
        return delegate.getValues(ref, keys);
      } catch (RuntimeException e) {
        throw handleRuntimeException(scope, "getValues", e);
      }
    }
  }

  @Override
  public Stream<Diff<VALUE>> getDiffs(Ref from, Ref to) throws ReferenceNotFoundException {
    Scope scope = createSpan("VersionStore.getDiffs")
        .withTag("nessie.version-store.operation", "GetDiffs")
        .withTag("nessie.version-store.from", safeToString(from))
        .withTag("to", safeToString(to))
        .startActive(true);
    try {
      return delegate.getDiffs(from, to).onClose(scope::close);
    } catch (RuntimeException e) {
      throw handleRuntimeExceptionClose(scope, "getDiffs", e);
    } finally {
      scope.span().log("getDiffs returns Stream");
    }
  }

  @Override
  public Collector collectGarbage() {
    try (Scope scope = createSpan("VersionStore.collectGarbage")
        .withTag("nessie.version-store.operation", "CollectGarbage")
        .startActive(true)) {
      try {
        return delegate.collectGarbage();
      } catch (RuntimeException e) {
        throw handleRuntimeException(scope, "collectGarbage", e);
      }
    }
  }

  @Override
  public Map<String, Supplier<Number>> gauges() {
    return delegate.gauges();
  }

  private static String safeToString(Object o) {
    return o != null ? o.toString() : "<null>";
  }

  private static String safeBranchName(BranchName branch) {
    return branch != null ? branch.getName() : "<null>";
  }

  private static int safeSize(Collection<?> collection) {
    return collection != null ? collection.size() : -1;
  }

  private Tracer getTracer() {
    return GlobalTracer.get();
  }

  private SpanBuilder createSpan(String name) {
    Tracer tracer = getTracer();
    return tracer.buildSpan(name)
        .asChildOf(tracer.activeSpan());
  }

  private static RuntimeException handleRuntimeException(Scope scope, String function, RuntimeException e) {
    Tags.ERROR.set(scope.span().log(ImmutableMap.of(Fields.EVENT, Tags.ERROR.getKey(),
        Fields.ERROR_OBJECT, e.toString())), true);
    LOGGER.debug("Failure in {}", function, e);
    return e;
  }

  private static RuntimeException handleRuntimeExceptionClose(Scope scope, String function, RuntimeException e) {
    Tags.ERROR.set(scope.span().log(ImmutableMap.of(Fields.EVENT, Tags.ERROR.getKey(),
        Fields.ERROR_OBJECT, e.toString())), true);
    LOGGER.debug("Failure in {}", function, e);
    scope.close();
    return e;
  }
}
