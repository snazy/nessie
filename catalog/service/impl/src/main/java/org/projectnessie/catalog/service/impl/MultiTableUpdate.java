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
package org.projectnessie.catalog.service.impl;

import java.util.ArrayList;
import java.util.List;
import org.projectnessie.catalog.model.snapshot.NessieEntitySnapshot;
import org.projectnessie.client.api.CommitMultipleOperationsBuilder;
import org.projectnessie.error.NessieConflictException;
import org.projectnessie.error.NessieNotFoundException;
import org.projectnessie.model.CommitResponse;
import org.projectnessie.model.Content;
import org.projectnessie.model.ContentKey;
import org.projectnessie.model.Operation;

/** Maintains state across all individual updates of a commit. */
final class MultiTableUpdate {
  private final CommitMultipleOperationsBuilder nessieCommit;
  private final List<SingleTableUpdate> tableUpdates = new ArrayList<>();
  private final List<String> storedLocations = new ArrayList<>();
  private volatile CommitResponse commitResponse;

  MultiTableUpdate(CommitMultipleOperationsBuilder nessieCommit) {
    this.nessieCommit = nessieCommit;
  }

  CommitMultipleOperationsBuilder nessieCommit() {
    return nessieCommit;
  }

  MultiTableUpdate commit() throws NessieConflictException, NessieNotFoundException {
    synchronized (this) {
      this.commitResponse = nessieCommit.commitWithResponse();
      return this;
    }
  }

  CommitResponse commitResponse() {
    synchronized (this) {
      return commitResponse;
    }
  }

  List<SingleTableUpdate> tableUpdates() {
    synchronized (this) {
      return tableUpdates;
    }
  }

  List<String> storedLocations() {
    synchronized (this) {
      return storedLocations;
    }
  }

  void addUpdate(ContentKey key, SingleTableUpdate singleTableUpdate) {
    synchronized (this) {
      tableUpdates.add(singleTableUpdate);
      nessieCommit.operation(Operation.Put.of(key, singleTableUpdate.content));
    }
  }

  void addStoredLocation(String location) {
    synchronized (this) {
      storedLocations.add(location);
    }
  }

  static final class SingleTableUpdate {
    final NessieEntitySnapshot<?> snapshot;
    final Content content;
    final ContentKey key;

    SingleTableUpdate(NessieEntitySnapshot<?> snapshot, Content content, ContentKey key) {
      this.snapshot = snapshot;
      this.content = content;
      this.key = key;
    }
  }
}
