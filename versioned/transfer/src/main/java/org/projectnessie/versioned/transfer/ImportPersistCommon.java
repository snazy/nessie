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
package org.projectnessie.versioned.transfer;

import static java.util.Objects.requireNonNull;
import static org.projectnessie.versioned.storage.common.indexes.StoreIndexElement.indexElement;
import static org.projectnessie.versioned.storage.common.logic.Logics.indexesLogic;
import static org.projectnessie.versioned.storage.common.objtypes.CommitOp.Action.ADD;
import static org.projectnessie.versioned.storage.common.objtypes.CommitOp.Action.REMOVE;
import static org.projectnessie.versioned.storage.common.objtypes.CommitOp.commitOp;
import static org.projectnessie.versioned.storage.common.objtypes.CommitOp.contentIdMaybe;
import static org.projectnessie.versioned.storage.common.objtypes.ContentValueObj.contentValue;
import static org.projectnessie.versioned.transfer.ProgressEvent.FINALIZE_PROGRESS;
import static org.projectnessie.versioned.transfer.ProgressEvent.TRANSITION_PROGRESS_COMMIT;

import java.io.IOException;
import java.io.InputStream;
import javax.annotation.Nonnull;
import org.projectnessie.model.Content;
import org.projectnessie.nessie.relocated.protobuf.ByteString;
import org.projectnessie.versioned.storage.common.config.StoreConfig;
import org.projectnessie.versioned.storage.common.exceptions.ObjNotFoundException;
import org.projectnessie.versioned.storage.common.indexes.StoreIndex;
import org.projectnessie.versioned.storage.common.indexes.StoreKey;
import org.projectnessie.versioned.storage.common.logic.IndexesLogic;
import org.projectnessie.versioned.storage.common.objtypes.CommitOp;
import org.projectnessie.versioned.storage.common.objtypes.ContentValueObj;
import org.projectnessie.versioned.storage.common.persist.Obj;
import org.projectnessie.versioned.storage.common.persist.ObjId;
import org.projectnessie.versioned.storage.common.persist.Persist;
import org.projectnessie.versioned.storage.transitional.TransitionalConfig;
import org.projectnessie.versioned.storage.transitional.TransitionalPersist;
import org.projectnessie.versioned.transfer.serialize.TransferTypes.Commit;
import org.projectnessie.versioned.transfer.serialize.TransferTypes.ExportMeta;
import org.projectnessie.versioned.transfer.serialize.TransferTypes.HeadsAndForks;
import org.projectnessie.versioned.transfer.serialize.TransferTypes.Operation;

abstract class ImportPersistCommon extends ImportCommon {

  private final TransitionalPersist transitionalPersist;
  private final Persist persist;

  ImportPersistCommon(ExportMeta exportMeta, NessieImporter importer) {
    super(exportMeta, importer);
    Persist intermediate = importer.intermediatePersist();
    this.transitionalPersist =
        intermediate != null
            ? TransitionalConfig.builder()
                .intermediatePersist(intermediate)
                .eventualPersist(requireNonNull(importer.persist()))
                .build()
                .createTransitionalPersist(StoreConfig.Adjustable.empty())
            : null;
    this.persist =
        transitionalPersist != null ? transitionalPersist : requireNonNull(importer.persist());
  }

  @Override
  boolean hasTransitionalStorage() {
    return importer.intermediatePersist() != null;
  }

  @Nonnull
  @jakarta.annotation.Nonnull
  Persist persist() {
    return persist;
  }

  @Override
  void importTransition() {
    transitionalPersist.transition(
        importer.commitBatchSize(),
        importer.referencesParallelism(),
        commits -> {
          ProgressListener l = importer.progressListener();
          synchronized (l) {
            for (int i = 0; i < commits; i++) {
              l.progress(TRANSITION_PROGRESS_COMMIT);
            }
          }
        },
        () -> {
          ProgressListener l = importer.progressListener();
          synchronized (l) {
            l.progress(FINALIZE_PROGRESS);
          }
        });
  }

  @Override
  void importFinalize(HeadsAndForks headsAndForks) {
    IndexesLogic indexesLogic = indexesLogic(persist());
    for (ByteString head : headsAndForks.getHeadsList()) {
      try {
        indexesLogic.completeIndexesInCommitChain(
            ObjId.objIdFromBytes(head),
            () -> importer.progressListener().progress(FINALIZE_PROGRESS));
      } catch (ObjNotFoundException e) {
        throw new RuntimeException(e);
      }
    }
  }

  @Override
  long importCommits() throws IOException {
    long commitCount = 0L;
    try (BatchWriter<Obj> batchWriter =
        BatchWriter.objWriter(importer.commitBatchSize(), persist())) {
      for (String fileName : exportMeta.getCommitsFilesList()) {
        try (InputStream input = importFiles.newFileInput(fileName)) {
          while (true) {
            Commit commit = Commit.parseDelimitedFrom(input);
            if (commit == null) {
              break;
            }
            processCommit(batchWriter, commit);
            commitCount++;
          }
        }
      }
    }
    return commitCount;
  }

  abstract void processCommit(BatchWriter<Obj> batchWriter, Commit commit) throws IOException;

  void processCommitOp(
      BatchWriter<Obj> batchWriter, StoreIndex<CommitOp> index, Operation op, StoreKey storeKey) {
    byte payload = (byte) op.getPayload();
    switch (op.getOperationType()) {
      case Delete:
        index.add(
            indexElement(
                storeKey, commitOp(REMOVE, payload, null, contentIdMaybe(op.getContentId()))));
        break;
      case Put:
        try (InputStream inValue = op.getValue().newInput()) {
          Content content = importer.objectMapper().readValue(inValue, Content.class);
          ByteString onRef =
              importer
                  .storeWorker()
                  .toStoreOnReferenceState(
                      content,
                      att -> {
                        throw new UnsupportedOperationException();
                      });

          ContentValueObj value = contentValue(op.getContentId(), payload, onRef);
          batchWriter.add(value);
          index.add(
              indexElement(
                  storeKey, commitOp(ADD, payload, value.id(), contentIdMaybe(op.getContentId()))));
        } catch (IOException e) {
          throw new RuntimeException(e);
        }
        break;
      default:
        throw new IllegalArgumentException("Unknown operation type " + op);
    }
  }
}
