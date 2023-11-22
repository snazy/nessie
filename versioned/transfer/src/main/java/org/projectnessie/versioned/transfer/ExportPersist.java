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

import static java.util.Collections.emptyMap;
import static java.util.Objects.requireNonNull;
import static java.util.Spliterators.spliteratorUnknownSize;
import static org.projectnessie.versioned.storage.common.logic.CommitLogQuery.commitLogQuery;
import static org.projectnessie.versioned.storage.common.logic.Logics.commitLogic;
import static org.projectnessie.versioned.storage.common.logic.Logics.indexesLogic;
import static org.projectnessie.versioned.storage.common.logic.Logics.referenceLogic;
import static org.projectnessie.versioned.storage.common.logic.Logics.repositoryLogic;
import static org.projectnessie.versioned.storage.common.logic.ReferencesQuery.referencesQuery;

import com.fasterxml.jackson.core.JsonProcessingException;
import java.io.IOException;
import java.util.ArrayDeque;
import java.util.Arrays;
import java.util.Deque;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;
import org.projectnessie.model.CommitMeta;
import org.projectnessie.model.Content;
import org.projectnessie.nessie.relocated.protobuf.ByteString;
import org.projectnessie.versioned.BranchName;
import org.projectnessie.versioned.NamedRef;
import org.projectnessie.versioned.TagName;
import org.projectnessie.versioned.storage.common.exceptions.ObjNotFoundException;
import org.projectnessie.versioned.storage.common.logic.CommitLogic;
import org.projectnessie.versioned.storage.common.logic.HeadsAndForkPoints;
import org.projectnessie.versioned.storage.common.logic.IdentifyHeadsAndForkPoints;
import org.projectnessie.versioned.storage.common.logic.IndexesLogic;
import org.projectnessie.versioned.storage.common.logic.PagedResult;
import org.projectnessie.versioned.storage.common.logic.ReferenceLogic;
import org.projectnessie.versioned.storage.common.logic.RepositoryDescription;
import org.projectnessie.versioned.storage.common.objtypes.CommitObj;
import org.projectnessie.versioned.storage.common.objtypes.CommitOp;
import org.projectnessie.versioned.storage.common.objtypes.ContentValueObj;
import org.projectnessie.versioned.storage.common.persist.Obj;
import org.projectnessie.versioned.storage.common.persist.ObjId;
import org.projectnessie.versioned.storage.common.persist.Persist;
import org.projectnessie.versioned.storage.common.persist.Reference;
import org.projectnessie.versioned.storage.versionstore.RefMapping;
import org.projectnessie.versioned.storage.versionstore.TypeMapping;
import org.projectnessie.versioned.transfer.files.ExportFileSupplier;
import org.projectnessie.versioned.transfer.serialize.Commit;
import org.projectnessie.versioned.transfer.serialize.ExportVersion;
import org.projectnessie.versioned.transfer.serialize.HeadsAndForks;
import org.projectnessie.versioned.transfer.serialize.NamedReference;
import org.projectnessie.versioned.transfer.serialize.Operation;
import org.projectnessie.versioned.transfer.serialize.OperationType;
import org.projectnessie.versioned.transfer.serialize.Ref;
import org.projectnessie.versioned.transfer.serialize.RefType;
import org.projectnessie.versioned.transfer.serialize.RepositoryDescriptionProto;

final class ExportPersist extends ExportCommon {

  private final ExportVersion exportVersion;

  ExportPersist(
      ExportFileSupplier exportFiles, NessieExporter exporter, ExportVersion exportVersion) {
    super(exportFiles, exporter);
    this.exportVersion = exportVersion;
  }

  @Override
  ExportVersion getExportVersion() {
    return exportVersion;
  }

  @Override
  HeadsAndForks exportCommits(ExportContext exportContext) {

    HeadsAndForkPoints headsAndForkPoints;
    try (Batcher<CommitObj> commitObjBatcher =
        new Batcher<>(
            exporter.commitBatchSize(), commits -> mapCommitObjs(commits, exportContext))) {
      headsAndForkPoints =
          exporter.fullScan()
              ? scanDatabase(commitObjBatcher::add)
              : scanAllReferences(commitObjBatcher::add);
    }

    HeadsAndForks.Builder hf =
        HeadsAndForks.newBuilder()
            .setScanStartedAtInMicros(headsAndForkPoints.getScanStartedAtInMicros());
    headsAndForkPoints.getHeads().forEach(h -> hf.addHeads(h.asBytes()));
    headsAndForkPoints.getForkPoints().forEach(h -> hf.addForkPoints(h.asBytes()));
    return hf.build();
  }

  private HeadsAndForkPoints scanAllReferences(Consumer<CommitObj> commitHandler) {
    Persist persist = persist();

    IdentifyHeadsAndForkPoints identify =
        new IdentifyHeadsAndForkPoints(
            exporter.expectedCommitCount(), persist.config().currentTimeMicros());

    ReferenceLogic referenceLogic = referenceLogic(persist);
    CommitLogic commitLogic = commitLogic(persist);
    String referencePrefix = exportVersion == ExportVersion.V2 ? null : RefMapping.REFS;
    referenceLogic
        .queryReferences(referencesQuery(referencePrefix))
        .forEachRemaining(
            ref -> {
              Deque<ObjId> commitsToProcess = new ArrayDeque<>();
              commitsToProcess.offerFirst(ref.pointer());
              while (!commitsToProcess.isEmpty()) {
                ObjId id = commitsToProcess.removeFirst();
                if (identify.isCommitNew(id)) {
                  Iterator<CommitObj> commitIter = commitLogic.commitLog(commitLogQuery(id));
                  while (commitIter.hasNext()) {
                    CommitObj commit = commitIter.next();
                    if (!identify.handleCommit(commit)) {
                      break;
                    }
                    commitHandler.accept(commit);
                    for (ObjId parentId : commit.secondaryParents()) {
                      if (identify.isCommitNew(parentId)) {
                        commitsToProcess.addLast(parentId);
                      }
                    }
                  }
                }
              }
            });

    return identify.finish();
  }

  private HeadsAndForkPoints scanDatabase(Consumer<CommitObj> commitHandler) {
    CommitLogic commitLogic = commitLogic(persist());
    return commitLogic.identifyAllHeadsAndForkPoints(exporter.expectedCommitCount(), commitHandler);
  }

  @Override
  void exportReferences(ExportContext exportContext) {
    ReferenceLogic referenceLogic = referenceLogic(persist());
    String referencePrefix = exportVersion == ExportVersion.V2 ? null : RefMapping.REFS;
    for (PagedResult<Reference, String> refs =
            referenceLogic.queryReferences(referencesQuery(referencePrefix));
        refs.hasNext(); ) {
      Reference reference = refs.next();
      if (exportVersion == ExportVersion.V1) {
        NamedRef namedRef = RefMapping.referenceToNamedRef(reference);
        NamedReference.Builder namedReference =
            NamedReference.newBuilder()
                .setRefType(refType(namedRef))
                .setName(namedRef.getName())
                .setCommitId(reference.pointer().asBytes());
        // reference.extendedInfoObj() cannot be exported in V1
        exportContext.writeNamedReference(namedReference.build());
      } else {
        ObjId extendedInfoObj = reference.extendedInfoObj();
        Ref.Builder refBuilder =
            Ref.newBuilder().setName(reference.name()).setPointer(reference.pointer().asBytes());
        if (extendedInfoObj != null) {
          refBuilder.setExtendedInfoObj(extendedInfoObj.asBytes());
        }
        exportContext.writeRef(refBuilder.build());
      }
      exporter.progressListener().progress(ProgressEvent.NAMED_REFERENCE_WRITTEN);
    }
  }

  private RefType refType(NamedRef namedRef) {
    if (namedRef instanceof TagName) {
      return RefType.Tag;
    }
    if (namedRef instanceof BranchName) {
      return RefType.Branch;
    }
    throw new IllegalArgumentException("Unknown named reference type " + namedRef);
  }

  @Override
  void writeRepositoryDescription() throws IOException {
    RepositoryDescription repositoryDescription =
        repositoryLogic(exporter.persist()).fetchRepositoryDescription();
    if (repositoryDescription != null) {
      writeRepositoryDescription(
          RepositoryDescriptionProto.newBuilder()
              .putAllProperties(repositoryDescription.properties())
              .setRepositoryId(persist().config().repositoryId())
              .setRepositoryCreatedTimestampMillis(
                  repositoryDescription.repositoryCreatedTime().toEpochMilli())
              .setOldestCommitTimestampMillis(
                  repositoryDescription.oldestPossibleCommitTime().toEpochMilli())
              .setDefaultBranchName(repositoryDescription.defaultBranchName())
              .build());
    }
  }

  private void mapCommitObjs(List<CommitObj> commitObjs, ExportContext exportContext) {
    IndexesLogic indexesLogic = indexesLogic(exporter.persist());

    Map<ObjId, Obj> objs = fetchReferencedObjs(commitObjs, indexesLogic);

    for (CommitObj c : commitObjs) {
      Commit commit = mapCommitObj(c, indexesLogic, objs);
      exportContext.writeCommit(commit);
      exporter.progressListener().progress(ProgressEvent.COMMIT_WRITTEN);
    }
  }

  private Map<ObjId, Obj> fetchReferencedObjs(
      List<CommitObj> commitObjs, IndexesLogic indexesLogic) {
    try {
      ObjId[] valueIds =
          commitObjs.stream()
              .flatMap(
                  c ->
                      StreamSupport.stream(
                              spliteratorUnknownSize(
                                  indexesLogic.commitOperations(c).iterator(), 0),
                              false)
                          .map(op -> op.content().value())
                          .filter(Objects::nonNull))
              .distinct()
              .toArray(ObjId[]::new);

      return valueIds.length > 0
          ? Arrays.stream(persist().fetchObjs(valueIds))
              .filter(Objects::nonNull)
              .collect(Collectors.toMap(Obj::id, Function.identity()))
          : emptyMap();
    } catch (ObjNotFoundException e) {
      throw new RuntimeException(e);
    }
  }

  private Commit mapCommitObj(CommitObj c, IndexesLogic indexesLogic, Map<ObjId, Obj> objs) {
    Commit.Builder b =
        Commit.newBuilder()
            .setCommitId(c.id().asBytes())
            .setParentCommitId(c.tail().get(0).asBytes())
            .setMessage(c.message())
            .setCommitSequence(c.seq())
            .setCreatedTimeMicros(c.created());

    if (exportVersion == ExportVersion.V1) {
      try {
        CommitMeta commitMeta = TypeMapping.toCommitMeta(c);
        byte[] commitMetaBytes = exporter.objectMapper().writeValueAsBytes(commitMeta);
        b.setMetadata(ByteString.copyFrom(commitMetaBytes));
      } catch (JsonProcessingException e) {
        throw new RuntimeException(e);
      }
    }

    c.headers()
        .keySet()
        .forEach(h -> b.addHeadersBuilder().setName(h).addAllValues(c.headers().getAll(h)));
    c.secondaryParents().forEach(p -> b.addAdditionalParents(p.asBytes()));
    indexesLogic
        .commitOperations(c)
        .forEach(
            op -> {
              CommitOp content = op.content();
              Operation.Builder opBuilder = b.addOperationsBuilder().setPayload(content.payload());

              if (exportVersion == ExportVersion.V1) {
                opBuilder.addAllContentKey(TypeMapping.storeKeyToKey(op.key()).getElements());
              } else {
                opBuilder.addContentKey(op.key().rawString());
              }

              ObjId valueId = content.value();
              if (valueId != null) {
                try {
                  ContentValueObj value = (ContentValueObj) objs.get(valueId);
                  Content modelContent =
                      exporter.storeWorker().valueFromStore((byte) content.payload(), value.data());
                  byte[] modelContentBytes =
                      exporter.objectMapper().writeValueAsBytes(modelContent);
                  opBuilder
                      .setContentId(value.contentId())
                      .setValue(ByteString.copyFrom(modelContentBytes));
                } catch (JsonProcessingException e) {
                  throw new RuntimeException(e);
                }
              }
              if (content.action().exists()) {
                opBuilder.setOperationType(OperationType.Put);
              } else {
                opBuilder.setOperationType(OperationType.Delete);
              }
            });
    return b.build();
  }

  private Persist persist() {
    return requireNonNull(exporter.persist());
  }
}
