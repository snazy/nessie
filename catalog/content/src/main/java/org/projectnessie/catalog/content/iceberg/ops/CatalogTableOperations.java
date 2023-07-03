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
package org.projectnessie.catalog.content.iceberg.ops;

import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;

import org.apache.iceberg.BaseMetastoreTableOperations;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.io.FileIO;

public class CatalogTableOperations extends BaseMetastoreTableOperations {
  private final FileIO fileIO;
  private final String tableName;

  private TableMetadata current;

  public CatalogTableOperations(FileIO fileIO, String tableName) {
    this.fileIO = fileIO;
    this.tableName = tableName;
  }

  private CatalogTableOperations(FileIO fileIO, String tableName, TableMetadata current) {
    this.fileIO = fileIO;
    this.tableName = tableName;
    this.current = current;
  }

  @Override
  protected String tableName() {
    return tableName;
  }

  @Override
  public FileIO io() {
    return fileIO;
  }

  @Override
  public TableMetadata current() {
    return requireNonNull(current, "No current TableMetadata");
  }

  @Override
  protected void doRefresh() {
    // no-op
  }

  @Override
  protected void doCommit(TableMetadata base, TableMetadata metadata) {
    requireNonNull(current, "No current TableMetadata");
    requireNonNull(metadata, "New TableMetadata must not be null");
    checkState(base == current, "Given 'base' is not the current metadata");
    current = metadata;
  }

  public CatalogTableOperations setCurrent(TableMetadata current) {
    this.current = requireNonNull(current, "New current TableMetadata must not be null");
    return this;
  }

  public CatalogTableOperations withCurrent(TableMetadata tableMetadata) {
    return new CatalogTableOperations(fileIO, tableName, tableMetadata);
  }
}
