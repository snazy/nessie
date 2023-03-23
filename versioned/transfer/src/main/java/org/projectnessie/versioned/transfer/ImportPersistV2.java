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

import static org.projectnessie.versioned.storage.common.indexes.StoreIndexes.newStoreIndex;
import static org.projectnessie.versioned.storage.common.indexes.StoreKey.keyFromString;
import static org.projectnessie.versioned.storage.common.logic.Logics.referenceLogic;
import static org.projectnessie.versioned.storage.common.logic.Logics.repositoryLogic;
import static org.projectnessie.versioned.storage.common.objtypes.CommitHeaders.newCommitHeaders;
import static org.projectnessie.versioned.storage.common.objtypes.CommitObj.commitBuilder;

import java.io.IOException;
import java.io.InputStream;
import java.time.Instant;
import org.projectnessie.versioned.storage.common.exceptions.RefAlreadyExistsException;
import org.projectnessie.versioned.storage.common.exceptions.RetryTimeoutException;
import org.projectnessie.versioned.storage.common.indexes.StoreIndex;
import org.projectnessie.versioned.storage.common.indexes.StoreKey;
import org.projectnessie.versioned.storage.common.logic.ReferenceLogic;
import org.projectnessie.versioned.storage.common.objtypes.CommitHeaders;
import org.projectnessie.versioned.storage.common.objtypes.CommitObj;
import org.projectnessie.versioned.storage.common.objtypes.CommitOp;
import org.projectnessie.versioned.storage.common.persist.Obj;
import org.projectnessie.versioned.storage.common.persist.ObjId;
import org.projectnessie.versioned.storage.common.persist.Persist;
import org.projectnessie.versioned.transfer.serialize.TransferTypes.Commit;
import org.projectnessie.versioned.transfer.serialize.TransferTypes.ExportMeta;
import org.projectnessie.versioned.transfer.serialize.TransferTypes.Ref;
import org.projectnessie.versioned.transfer.serialize.TransferTypes.RepositoryDescriptionProto;

final class ImportPersistV2 extends ImportPersistCommon {

  ImportPersistV2(ExportMeta exportMeta, NessieImporter importer) {
    super(exportMeta, importer);
  }

  @Override
  void prepareRepository() throws IOException {
    RepositoryDescriptionProto repositoryDescription = importer.loadRepositoryDescription();

    Persist persist = persist();
    persist.erase();
    repositoryLogic(persist)
        .initialize(
            repositoryDescription.getDefaultBranchName(),
            false,
            b -> {
              if (repositoryDescription.hasOldestCommitTimestampMillis()) {
                b.oldestPossibleCommitTime(
                    Instant.ofEpochMilli(repositoryDescription.getOldestCommitTimestampMillis()));
              }
              b.putAllProperties(repositoryDescription.getPropertiesMap());
            });
  }

  @Override
  long importNamedReferences() throws IOException {
    long namedReferenceCount = 0L;
    ReferenceLogic refLogic = referenceLogic(persist());
    for (String fileName : exportMeta.getNamedReferencesFilesList()) {
      try (InputStream input = importFiles.newFileInput(fileName)) {
        while (true) {
          Ref ref = Ref.parseDelimitedFrom(input);
          if (ref == null) {
            break;
          }

          try {
            refLogic.createReference(ref.getName(), ObjId.objIdFromBytes(ref.getPointer()));
          } catch (RefAlreadyExistsException | RetryTimeoutException e) {
            throw new RuntimeException(e);
          }

          namedReferenceCount++;
          importer.progressListener().progress(ProgressEvent.NAMED_REFERENCE_WRITTEN);
        }
      }
    }
    return namedReferenceCount;
  }

  @Override
  void processCommit(BatchWriter<Obj> batchWriter, Commit commit) {
    CommitHeaders.Builder headers = newCommitHeaders();
    commit
        .getHeadersList()
        .forEach(h -> h.getValuesList().forEach(v -> headers.add(h.getName(), v)));

    CommitObj.Builder c =
        commitBuilder()
            .id(ObjId.objIdFromBytes(commit.getCommitId()))
            .addTail(ObjId.objIdFromBytes(commit.getParentCommitId()))
            .created(commit.getCreatedTimeMicros())
            .seq(commit.getCommitSequence())
            .message(commit.getMessage())
            .headers(headers.build())
            .incompleteIndex(true);
    commit
        .getAdditionalParentsList()
        .forEach(ap -> c.addSecondaryParents(ObjId.objIdFromBytes(ap)));
    StoreIndex<CommitOp> index = newStoreIndex(CommitOp.COMMIT_OP_SERIALIZER);
    commit
        .getOperationsList()
        .forEach(
            op -> {
              StoreKey storeKey = keyFromString(op.getContentKey(0));
              processCommitOp(batchWriter, index, op, storeKey);
            });

    c.incrementalIndex(index.serialize());

    batchWriter.add(c.build());

    importer.progressListener().progress(ProgressEvent.COMMIT_WRITTEN);
  }
}
