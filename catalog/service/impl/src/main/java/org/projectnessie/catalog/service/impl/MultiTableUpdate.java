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

import static com.google.common.base.Preconditions.checkState;
import static org.projectnessie.versioned.CheckedOperation.checkedOperation;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.projectnessie.catalog.model.snapshot.NessieEntitySnapshot;
import org.projectnessie.error.NessieConflictException;
import org.projectnessie.error.NessieNotFoundException;
import org.projectnessie.model.Branch;
import org.projectnessie.model.CommitMeta;
import org.projectnessie.model.CommitResponse;
import org.projectnessie.model.Content;
import org.projectnessie.model.ContentKey;
import org.projectnessie.model.Operation.Put;
import org.projectnessie.services.authz.AccessCheckParams;
import org.projectnessie.services.spi.TreeService;
import org.projectnessie.versioned.CheckedOperation;

/** Maintains state across all individual updates of a commit. */
final class MultiTableUpdate {
  private final TreeService treeService;
  private final List<CheckedOperation> operations = new ArrayList<>();
  private final List<SingleTableUpdate> tableUpdates = new ArrayList<>();
  private final List<String> storedLocations = new ArrayList<>();
  private Map<ContentKey, String> addedContentsMap;
  private Branch targetBranch;
  private boolean committed;

  MultiTableUpdate(TreeService treeService, Branch target) {
    this.treeService = treeService;
    this.targetBranch = target;
  }

  MultiTableUpdate commit(CommitMeta commitMeta)
      throws NessieConflictException, NessieNotFoundException {
    synchronized (this) {
      committed = true;
      if (!tableUpdates.isEmpty()) {
        CommitResponse commitResponse =
            treeService.commitMultipleOperations(
                targetBranch().getName(), targetBranch.getHash(), commitMeta, operations);

        addedContentsMap =
            commitResponse.getAddedContents() != null
                ? commitResponse.toAddedContentsMap()
                : Map.of();
        targetBranch = commitResponse.getTargetBranch();
      }
      return this;
    }
  }

  Branch targetBranch() {
    synchronized (this) {
      return targetBranch;
    }
  }

  Map<ContentKey, String> addedContentsMap() {
    synchronized (this) {
      return addedContentsMap != null ? addedContentsMap : Map.of();
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
    checkState(!committed, "Already committed");
    synchronized (this) {
      tableUpdates.add(singleTableUpdate);
      operations.add(
          checkedOperation(
              Put.of(key, singleTableUpdate.content), singleTableUpdate.accessCheckParams));
    }
  }

  void addStoredLocation(String location) {
    checkState(!committed, "Already committed");
    synchronized (this) {
      storedLocations.add(location);
    }
  }

  static final class SingleTableUpdate {
    final NessieEntitySnapshot<?> snapshot;
    final Content content;
    final ContentKey key;
    final AccessCheckParams accessCheckParams;

    SingleTableUpdate(
        NessieEntitySnapshot<?> snapshot,
        Content content,
        ContentKey key,
        AccessCheckParams accessCheckParams) {
      this.snapshot = snapshot;
      this.content = content;
      this.key = key;
      this.accessCheckParams = accessCheckParams;
    }
  }
}
