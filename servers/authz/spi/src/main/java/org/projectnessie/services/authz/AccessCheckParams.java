/*
 * Copyright (C) 2024 Dremio
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
package org.projectnessie.services.authz;

import static org.projectnessie.services.authz.Check.Component.ALL_SET;

import com.google.errorprone.annotations.CanIgnoreReturnValue;
import java.util.Map;
import java.util.Set;
import org.immutables.value.Value;
import org.projectnessie.model.ContentKey;

@Value.Immutable
@Value.Style(allParameters = true)
public interface AccessCheckParams {

  AccessCheckParams NESSIE_API_READ_ONLY = withComponents(false, ALL_SET);
  AccessCheckParams NESSIE_API_FOR_WRITE = withComponents(true, ALL_SET);
  AccessCheckParams CATALOG_CONTENT_CHECK_EXISTS = withComponents(false, Set.of());
  AccessCheckParams CATALOG_CONTENT_CHECK_FOR_READ = withComponents(false, Set.of());
  AccessCheckParams CATALOG_CONTENT_CHECK_FOR_CREATE = withComponents(true, Set.of());
  AccessCheckParams CATALOG_CONTENT_CHECK_FOR_DROP = withComponents(true, Set.of());
  AccessCheckParams CATALOG_CONTENT_CHECK_FOR_UPDATE = withComponents(true, Set.of());
  AccessCheckParams CATALOG_CONTENT_CHECK_FOR_RENAME_FROM = withComponents(true, Set.of());
  AccessCheckParams CATALOG_CONTENT_CHECK_FOR_RENAME_TO = withComponents(true, Set.of());
  AccessCheckParams CATALOG_CONTENT_READ_FOR_COMMIT = withComponents(true, Set.of());

  AccessCheckParams CATALOG_CONTENT_CHECK_FOR_FILE_READ = withComponents(false, Set.of());
  AccessCheckParams CATALOG_CONTENT_CHECK_FOR_FILE_WRITE = withComponents(true, Set.of());

  /**
   * If {@code false}, read access checks will be performed. If {@code true}, update/create access
   * checks will be performed, potentially in addition to the read access checks.
   */
  boolean forWrite();

  Set<Check.Component> components();

  Map<ContentKey, Set<Check.Component>> componentsByKey();

  static AccessCheckParams nessieApi(boolean forWrite) {
    return forWrite ? NESSIE_API_FOR_WRITE : NESSIE_API_READ_ONLY;
  }

  static AccessCheckParams withComponents(boolean forWrite, Set<Check.Component> components) {
    return ImmutableAccessCheckParams.of(forWrite, components, Map.of());
  }

  static AccessCheckParams withComponentByKey(
      boolean forWrite, Map<ContentKey, Set<Check.Component>> componentsByKey) {
    return ImmutableAccessCheckParams.of(forWrite, Set.of(), componentsByKey);
  }

  static Builder builder() {
    return ImmutableAccessCheckParams.builder();
  }

  interface Builder {
    @CanIgnoreReturnValue
    Builder from(AccessCheckParams instance);

    @CanIgnoreReturnValue
    Builder forWrite(boolean forWrite);

    @CanIgnoreReturnValue
    Builder addComponents(Check.Component element);

    @CanIgnoreReturnValue
    Builder addComponents(Check.Component... elements);

    @CanIgnoreReturnValue
    Builder addAllComponents(Iterable<? extends Check.Component> elements);

    @CanIgnoreReturnValue
    Builder components(Iterable<? extends Check.Component> elements);

    @CanIgnoreReturnValue
    Builder putComponentsByKey(ContentKey key, Set<Check.Component> value);

    AccessCheckParams build();
  }
}
