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

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.projectnessie.versioned.impl.condition.ConditionExpression;
import org.projectnessie.versioned.impl.condition.UpdateExpression;
import org.projectnessie.versioned.store.Id;
import org.projectnessie.versioned.store.LoadStep;
import org.projectnessie.versioned.store.NotFoundException;
import org.projectnessie.versioned.store.SaveOp;
import org.projectnessie.versioned.store.Store;
import org.projectnessie.versioned.store.Store.Acceptor;
import org.projectnessie.versioned.store.ValueType;
import org.projectnessie.versioned.tiered.BaseValue;

final class Persistence {
  private final Store store;

  private final PersistenceCache cache;

  Persistence(Store store, TieredVersionStoreConfig config) {
    this.store = store;
    this.cache = PersistenceCache.forConfig(config.cache());
  }

  <C extends BaseValue<C>, E extends PersistentBase<C>> boolean putIfAbsent(ValueType<C> type, E value) {
    boolean result = store.putIfAbsent(new EntitySaveOp<>(type, value));
    if (result) {
      cache.add(value);
    }
    return result;
  }

  <C extends BaseValue<C>, E extends PersistentBase<C>> void put(ValueType<C> type, E value, Optional<ConditionExpression> condition) {
    store.put(new EntitySaveOp<>(type, value), condition);
    cache.add(value);
  }

  <C extends BaseValue<C>> boolean delete(ValueType<C> type, Id id, Optional<ConditionExpression> condition) {
    boolean result = store.delete(type, id, condition);
    if (result) {
      cache.remove(type, id);
    }
    return result;
  }

  @SuppressWarnings({"rawtypes", "unchecked"})
  void save(List<PersistentBase<?>> values) {
    List<PersistentBase<?>> cacheAdds = new ArrayList<>(values.size());
    try {
      List<SaveOp> saveOpsRaw = values.stream()
          .filter(cache::hasNoSaved) // do not save items that are already saved
          .map(PersistentBase::cleanup)
          .peek(cacheAdds::add)
          .peek(cache::addUnsaved) // add values to save as "unsaved" to the cache
          .map(v -> {
            EntityType entityType = EntityType.forType(v.valueType());
            return entityType.createSaveOpForEntity(v);
          })
          .map(SaveOp.class::cast)
          .collect(Collectors.toList());
      List<SaveOp<?>> saveOps = (List) saveOpsRaw;
      store.save(saveOps);
      // re-add the persisted values as "saved" to the cache
      cacheAdds.forEach(cache::add);
    } catch (RuntimeException e) {
      // remove items from cache, that could not be saved
      cacheAdds.forEach(p -> cache.remove(p.valueType(), p.getId()));
      throw e;
    }
  }

  @SuppressWarnings("unchecked")
  <C extends BaseValue<C>, E extends PersistentBase<C>> E loadSingle(ValueType<C> type, Id id) {
    E v = cache.get(type, id);
    if (v == null) {
      v = (E) EntityType.forType(type).loadSingle(store, id);
      cache.add(v);
    }
    return v;
  }

  void load(LoadStep loadstep) {
    store.load(cache.loadstep(loadstep));
  }

  <C extends BaseValue<C>> boolean update(ValueType<C> type, Id id, UpdateExpression update,
      Optional<ConditionExpression> condition, Optional<BaseValue<C>> consumer) throws NotFoundException {
    if (type.isImmutable()) {
      throw new IllegalArgumentException(String.format("Type %s is immutable", type));
    }
    return store.update(type, id, update, condition, consumer);
  }

  <C extends BaseValue<C>> Stream<Acceptor<C>> getValues(ValueType<C> type) {
    return store.getValues(type);
  }
}
