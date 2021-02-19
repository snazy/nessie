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
package org.projectnessie.versioned.impl;

import java.util.Optional;
import java.util.stream.Stream;

import org.projectnessie.versioned.impl.TieredVersionStoreConfig.CacheConfig;
import org.projectnessie.versioned.store.Id;
import org.projectnessie.versioned.store.LoadOp;
import org.projectnessie.versioned.store.LoadStep;
import org.projectnessie.versioned.store.ValueType;
import org.projectnessie.versioned.tiered.BaseValue;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;

import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.composite.CompositeMeterRegistry;

abstract class PersistenceCache {

  static PersistenceCache forConfig(CacheConfig config) {
    if (!config.enabled()) {
      return new NoCache();
    }

    return new TypedCache(config);
  }

  abstract <C extends BaseValue<C>, E extends PersistentBase<C>> void add(E value);

  abstract <C extends BaseValue<C>, E extends PersistentBase<C>> void addUnsaved(E value);

  abstract <C extends BaseValue<C>> void remove(ValueType<C> type, Id id);

  abstract <E extends PersistentBase<C>, C extends BaseValue<C>> E get(ValueType<C> type, Id id);

  abstract LoadStep loadstep(LoadStep loadstep);

  abstract <E extends PersistentBase<C>, C extends BaseValue<C>> boolean hasNoSaved(E value);

  private static final class TypedCache extends PersistenceCache {

    final Cache<CacheKey, CacheEntry> cache;

    TypedCache(CacheConfig config) {
      Caffeine<Object, Object> cacheBuilder = Caffeine.newBuilder();
      if (config.recordStats()) {
        cacheBuilder.recordStats();
      }
      if (config.maxSize() > 0) {
        cacheBuilder.maximumSize(config.maxSize());
      }
      cache = cacheBuilder.build();

      CompositeMeterRegistry r = Metrics.globalRegistry;
      r.gauge("nessie.versioned.tiered.cache.eviction-count", cache, c -> c.stats().evictionCount());
      r.gauge("nessie.versioned.tiered.cache.eviction-weight", cache, c -> c.stats().evictionWeight());
      r.gauge("nessie.versioned.tiered.cache.hit-count", cache, c -> c.stats().hitCount());
      r.gauge("nessie.versioned.tiered.cache.hit-rate", cache, c -> c.stats().hitRate());
      r.gauge("nessie.versioned.tiered.cache.miss-count", cache, c -> c.stats().missCount());
      r.gauge("nessie.versioned.tiered.cache.miss.rate", cache, c -> c.stats().missRate());
      r.gauge("nessie.versioned.tiered.cache.request-count", cache, c -> c.stats().requestCount());
      r.gauge("nessie.versioned.tiered.cache.estimated-size", cache, Cache::estimatedSize);
    }

    @Override
    public <C extends BaseValue<C>, E extends PersistentBase<C>> void add(E value) {
      if (!value.valueType().isImmutable()) {
        return;
      }
      cache.put(key(value), CacheEntry.saved(value));
    }

    @Override
    public <C extends BaseValue<C>, E extends PersistentBase<C>> void addUnsaved(E value) {
      if (!value.valueType().isImmutable()) {
        return;
      }
      cache.get(key(value), k -> CacheEntry.unsaved(value));
    }

    @Override
    public <C extends BaseValue<C>> void remove(ValueType<C> type, Id id) {
      if (!type.isImmutable()) {
        return;
      }

      cache.invalidate(key(type, id));
    }

    @Override
    public <E extends PersistentBase<C>, C extends BaseValue<C>> boolean hasNoSaved(E value) {
      CacheEntry entry = cache.getIfPresent(key(value));
      return entry == null || !entry.isSaved();
    }

    @SuppressWarnings("unchecked")
    @Override
    public <E extends PersistentBase<C>, C extends BaseValue<C>> E get(ValueType<C> type, Id id) {
      if (!type.isImmutable()) {
        return null;
      }

      CacheEntry entry = cache.getIfPresent(key(type, id));
      return entry != null ? (E) entry.element : null;
    }

    @Override
    public LoadStep loadstep(LoadStep loadstep) {
      return new CacheLoadStep(loadstep);
    }

    private static final class CacheEntry {
      private volatile boolean saved;
      private final PersistentBase<?> element;

      private CacheEntry(PersistentBase<?> element) {
        this.element = element;
      }

      static <E extends PersistentBase<C>, C extends BaseValue<C>> CacheEntry saved(E value) {
        CacheEntry entry = new CacheEntry(value);
        entry.saved = true;
        return entry;
      }

      static <E extends PersistentBase<C>, C extends BaseValue<C>> CacheEntry unsaved(E value) {
        return new CacheEntry(value);
      }

      boolean isSaved() {
        return saved;
      }

      PersistentBase<?> getElement() {
        return element;
      }
    }

    private final class CacheLoadStep implements LoadStep {

      private final LoadStep delegate;

      public CacheLoadStep(LoadStep delegate) {
        this.delegate = delegate;
      }

      private <E extends PersistentBase<C>, C extends BaseValue<C>> boolean filter(LoadOp<C> op) {
        CacheEntry e = cache.getIfPresent(key(op.getValueType(), op.getId()));
        if (e != null) {
          @SuppressWarnings("unchecked") E value = (E) e.getElement();
          value.applyToConsumer(op.getReceiver());
          op.done();
          return false;
        }
        return true;
      }

      @Override
      public Stream<LoadOp<?>> getOps() {
        return delegate.getOps()
            .filter(this::filter);
      }

      @Override
      public Optional<LoadStep> getNext() {
        return delegate.getNext().map(CacheLoadStep::new);
      }

      @Override
      public LoadStep combine(LoadStep other) {
        throw new UnsupportedOperationException();
      }
    }
  }

  private static final class NoCache extends PersistenceCache {

    @Override
    public <C extends BaseValue<C>, E extends PersistentBase<C>> void add(E value) {
    }

    @Override
    public <C extends BaseValue<C>, E extends PersistentBase<C>> void addUnsaved(E value) {
    }

    @Override
    public <C extends BaseValue<C>> void remove(ValueType<C> type, Id id) {
    }

    @Override
    public <E extends PersistentBase<C>, C extends BaseValue<C>> boolean hasNoSaved(E value) {
      return true;
    }

    @Override
    public <E extends PersistentBase<C>, C extends BaseValue<C>> E get(ValueType<C> type, Id id) {
      return null;
    }

    @Override
    public LoadStep loadstep(LoadStep loadstep) {
      return loadstep;
    }
  }

  private static CacheKey key(PersistentBase<?> value) {
    return new CacheKey(value.valueType(), value.getId());
  }

  private static CacheKey key(ValueType<?> type, Id id) {
    return new CacheKey(type, id);
  }

  private static final class CacheKey {
    final ValueType<?> type;
    final Id id;

    CacheKey(ValueType<?> type, Id id) {
      this.type = type;
      this.id = id;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      CacheKey cacheKey = (CacheKey) o;
      return type.equals(cacheKey.type) && id.equals(cacheKey.id);
    }

    @Override
    public int hashCode() {
      return type.hashCode() * 31 + id.hashCode();
    }
  }
}
