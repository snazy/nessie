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

import static org.projectnessie.versioned.VersionStore.KeyRestrictions.NO_KEY_RESTRICTIONS;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.google.common.hash.Hasher;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.projectnessie.model.CommitMeta;
import org.projectnessie.model.Content;
import org.projectnessie.model.ContentKey;
import org.projectnessie.nessie.relocated.protobuf.ByteString;
import org.projectnessie.versioned.CommitMetaSerializer;
import org.projectnessie.versioned.ContentResult;
import org.projectnessie.versioned.GetNamedRefsParams;
import org.projectnessie.versioned.KeyEntry;
import org.projectnessie.versioned.ReferenceInfo;
import org.projectnessie.versioned.ReferenceNotFoundException;
import org.projectnessie.versioned.VersionStore;
import org.projectnessie.versioned.paging.PaginationIterator;
import org.projectnessie.versioned.persist.adapter.spi.AbstractDatabaseAdapter;
import org.projectnessie.versioned.storage.common.objtypes.Hashes;
import org.projectnessie.versioned.store.DefaultStoreWorker;
import org.projectnessie.versioned.transfer.files.ExportFileSupplier;
import org.projectnessie.versioned.transfer.serialize.Commit;
import org.projectnessie.versioned.transfer.serialize.ExportVersion;
import org.projectnessie.versioned.transfer.serialize.HeadsAndForks;
import org.projectnessie.versioned.transfer.serialize.NamedReference;
import org.projectnessie.versioned.transfer.serialize.Operation;
import org.projectnessie.versioned.transfer.serialize.OperationType;
import org.projectnessie.versioned.transfer.serialize.RefType;

/**
 * Creates export artifacts by generating artificial commits from the latest content object values
 * on one particular branch.
 */
final class ExportContents extends ExportCommon {
  private final VersionStore store;

  private ByteString lastCommitId = AbstractDatabaseAdapter.NO_ANCESTOR.asBytes();

  ExportContents(ExportFileSupplier exportFiles, NessieExporter exporter) {
    super(exportFiles, exporter);
    store = exporter.versionStore();
  }

  @Override
  ExportVersion getExportVersion() {
    return ExportVersion.V1;
  }

  @Override
  void writeRepositoryDescription() {}

  private <T> List<T> take(int n, Iterator<T> it) {
    List<T> result = new ArrayList<>(n);
    while (n-- > 0 && it.hasNext()) {
      result.add(it.next());
    }
    return result;
  }

  @SuppressWarnings("UnstableApiUsage")
  @Override
  HeadsAndForks exportCommits(ExportContext exportContext) {
    ReferenceInfo<CommitMeta> ref;
    try {
      ref = store.getNamedRef(exporter.contentsFromBranch(), GetNamedRefsParams.DEFAULT);
    } catch (ReferenceNotFoundException e) {
      throw new RuntimeException(e);
    }

    long startMicors = TimeUnit.MILLISECONDS.toMicros(currentTimestampMillis());

    long seq = 0;
    try (PaginationIterator<KeyEntry> entries =
        store.getKeys(ref.getNamedRef(), null, false, NO_KEY_RESTRICTIONS)) {
      while (true) {
        List<KeyEntry> batch = take(exporter.contentsBatchSize(), entries);
        if (batch.isEmpty()) {
          break;
        }

        ByteString meta =
            CommitMetaSerializer.METADATA_SERIALIZER.toBytes(
                CommitMeta.fromMessage(
                    String.format(
                        "Single branch export from '%s', part %d",
                        ref.getNamedRef().getName(), seq + 1)));
        long micros = TimeUnit.MILLISECONDS.toMicros(currentTimestampMillis());

        Hasher hasher = Hashes.newHasher();
        hasher.putBytes(meta.asReadOnlyByteBuffer());
        hasher.putBytes(lastCommitId.asReadOnlyByteBuffer());
        hasher.putLong(seq);
        hasher.putLong(micros);

        Commit.Builder commitBuilder =
            Commit.newBuilder()
                .setMetadata(meta)
                .setCommitSequence(seq++)
                .setCreatedTimeMicros(micros)
                .setParentCommitId(lastCommitId);

        Map<ContentKey, ContentResult> values =
            store.getValues(
                ref.getHash(),
                batch.stream().map(e -> e.getKey().contentKey()).collect(Collectors.toList()));
        for (Map.Entry<ContentKey, ContentResult> entry : values.entrySet()) {
          Operation op = putOperationFromCommit(entry.getKey(), entry.getValue().content()).build();
          hasher.putBytes(op.toByteArray());
          commitBuilder.addOperations(op);
        }

        ByteString commitId = ByteString.copyFrom(hasher.hash().asBytes());
        commitBuilder.setCommitId(commitId);

        lastCommitId = commitId;
        exportContext.writeCommit(commitBuilder.build());
        exporter.progressListener().progress(ProgressEvent.COMMIT_WRITTEN);
      }
    } catch (ReferenceNotFoundException e) {
      throw new RuntimeException(e);
    }

    return HeadsAndForks.newBuilder()
        .setScanStartedAtInMicros(startMicors)
        .addHeads(lastCommitId)
        .build();
  }

  @Override
  void exportReferences(ExportContext exportContext) {
    exportContext.writeNamedReference(
        NamedReference.newBuilder()
            .setRefType(RefType.Branch)
            .setCommitId(lastCommitId)
            .setName(exporter.contentsFromBranch())
            .build());
    exporter.progressListener().progress(ProgressEvent.NAMED_REFERENCE_WRITTEN);
  }

  private Operation.Builder putOperationFromCommit(ContentKey key, Content value) {
    return Operation.newBuilder()
        .setOperationType(OperationType.Put)
        .addAllContentKey(key.getElements())
        .setContentId(value.getId())
        .setPayload(DefaultStoreWorker.payloadForContent(value))
        .setValue(contentToValue(value));
  }

  private ByteString contentToValue(Content content) {
    try {
      return ByteString.copyFromUtf8(exporter.objectMapper().writeValueAsString(content));
    } catch (JsonProcessingException e) {
      throw new RuntimeException(e);
    }
  }
}
