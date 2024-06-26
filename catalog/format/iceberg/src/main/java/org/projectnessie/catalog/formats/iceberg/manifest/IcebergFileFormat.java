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
package org.projectnessie.catalog.formats.iceberg.manifest;

public enum IcebergFileFormat {
  ORC("orc", true),
  PARQUET("parquet", true),
  AVRO("avro", true),
  METADATA("metadata.json", false),
  ;

  private final String fileExtension;
  private final boolean splittable;

  IcebergFileFormat(String fileExtension, boolean splittable) {
    this.fileExtension = "." + fileExtension;
    this.splittable = splittable;
  }

  public boolean splittable() {
    return splittable;
  }

  public String fileExtension() {
    return fileExtension;
  }
}
