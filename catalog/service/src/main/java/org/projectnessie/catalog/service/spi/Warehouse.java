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
package org.projectnessie.catalog.service.spi;

import java.util.Map;
import org.apache.iceberg.io.FileIO;
import org.immutables.value.Value;
import org.projectnessie.catalog.content.iceberg.metadata.DelegatingMetadataIO;
import org.projectnessie.catalog.content.iceberg.metadata.MetadataIO;

@Value.Immutable
public interface Warehouse {
  String name();

  String location();

  Map<String, String> configDefaults();

  Map<String, String> configOverrides();

  @Value.Default
  default MetadataIO metadataIO() {
    return new DelegatingMetadataIO(fileIO());
  }

  FileIO fileIO();

  static ImmutableWarehouse.Builder builder() {
    return ImmutableWarehouse.builder();
  }
}
