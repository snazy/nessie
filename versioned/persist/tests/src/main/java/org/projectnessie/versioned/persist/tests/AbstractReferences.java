/*
 * Copyright (C) 2020 Dremio
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
package org.projectnessie.versioned.persist.tests;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.groups.Tuple.tuple;
import static org.junit.jupiter.api.Assertions.assertAll;
import static org.projectnessie.versioned.store.DefaultStoreWorker.payloadForContent;
import static org.projectnessie.versioned.testworker.OnRefOnly.onRef;

import com.google.protobuf.ByteString;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.IntFunction;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import org.assertj.core.groups.Tuple;
import org.junit.jupiter.api.Test;
import org.projectnessie.versioned.BranchName;
import org.projectnessie.versioned.GetNamedRefsParams;
import org.projectnessie.versioned.Hash;
import org.projectnessie.versioned.Key;
import org.projectnessie.versioned.NamedRef;
import org.projectnessie.versioned.ReferenceAlreadyExistsException;
import org.projectnessie.versioned.ReferenceConflictException;
import org.projectnessie.versioned.ReferenceInfo;
import org.projectnessie.versioned.ReferenceNotFoundException;
import org.projectnessie.versioned.TagName;
import org.projectnessie.versioned.persist.adapter.ContentId;
import org.projectnessie.versioned.persist.adapter.DatabaseAdapter;
import org.projectnessie.versioned.persist.adapter.ImmutableCommitParams;
import org.projectnessie.versioned.persist.adapter.KeyWithBytes;
import org.projectnessie.versioned.persist.adapter.RefLog;
import org.projectnessie.versioned.store.DefaultStoreWorker;
import org.projectnessie.versioned.testworker.OnRefOnly;

/** Verifies handling of repo-description in the database-adapters. */
public abstract class AbstractReferences {

  private final DatabaseAdapter databaseAdapter;

  protected AbstractReferences(DatabaseAdapter databaseAdapter) {
    this.databaseAdapter = databaseAdapter;
  }

  @Test
  void createBranch() throws Exception {
    BranchName create = BranchName.of("createBranch");
    createNamedRef(create, TagName.of(create.getName()));
  }

  @Test
  void createTag() throws Exception {
    TagName create = TagName.of("createTag");
    createNamedRef(create, BranchName.of(create.getName()));
  }

  private void createNamedRef(NamedRef create, NamedRef opposite) throws Exception {
    BranchName branch = BranchName.of("main");

    try (Stream<ReferenceInfo<ByteString>> refs =
        databaseAdapter.namedRefs(GetNamedRefsParams.DEFAULT)) {
      assertThat(refs.map(ReferenceInfo::getNamedRef)).containsExactlyInAnyOrder(branch);
    }

    Hash mainHash = databaseAdapter.hashOnReference(branch, Optional.empty());

    assertThatThrownBy(() -> databaseAdapter.hashOnReference(create, Optional.empty()))
        .isInstanceOf(ReferenceNotFoundException.class);

    Hash createHash =
        databaseAdapter.create(create, databaseAdapter.hashOnReference(branch, Optional.empty()));
    assertThat(createHash).isEqualTo(mainHash);

    try (Stream<ReferenceInfo<ByteString>> refs =
        databaseAdapter.namedRefs(GetNamedRefsParams.DEFAULT)) {
      assertThat(refs.map(ReferenceInfo::getNamedRef)).containsExactlyInAnyOrder(branch, create);
    }

    assertThatThrownBy(
            () ->
                databaseAdapter.create(
                    create, databaseAdapter.hashOnReference(branch, Optional.empty())))
        .isInstanceOf(ReferenceAlreadyExistsException.class);

    assertThat(databaseAdapter.hashOnReference(create, Optional.empty())).isEqualTo(createHash);
    assertThatThrownBy(() -> databaseAdapter.hashOnReference(opposite, Optional.empty()))
        .isInstanceOf(ReferenceNotFoundException.class);

    assertThatThrownBy(
            () ->
                databaseAdapter.create(
                    BranchName.of(create.getName()),
                    databaseAdapter.hashOnReference(branch, Optional.empty())))
        .isInstanceOf(ReferenceAlreadyExistsException.class);

    assertThatThrownBy(
            () -> databaseAdapter.delete(create, Optional.of(Hash.of("dead00004242fee18eef"))))
        .isInstanceOf(ReferenceConflictException.class);

    assertThatThrownBy(() -> databaseAdapter.delete(opposite, Optional.of(createHash)))
        .isInstanceOf(ReferenceNotFoundException.class);

    databaseAdapter.delete(create, Optional.of(createHash));

    assertThatThrownBy(() -> databaseAdapter.hashOnReference(create, Optional.empty()))
        .isInstanceOf(ReferenceNotFoundException.class);

    try (Stream<ReferenceInfo<ByteString>> refs =
        databaseAdapter.namedRefs(GetNamedRefsParams.DEFAULT)) {
      assertThat(refs.map(ReferenceInfo::getNamedRef)).containsExactlyInAnyOrder(branch);
    }
  }

  @Test
  void verifyNotFoundAndConflictExceptionsForUnreachableCommit() throws Exception {
    BranchName main = BranchName.of("main");
    BranchName unreachable = BranchName.of("unreachable");
    BranchName helper = BranchName.of("helper");

    databaseAdapter.create(unreachable, databaseAdapter.hashOnReference(main, Optional.empty()));
    Hash helperHead =
        databaseAdapter.create(helper, databaseAdapter.hashOnReference(main, Optional.empty()));

    OnRefOnly hello = onRef("hello", "contentId");
    Hash unreachableHead =
        databaseAdapter.commit(
            ImmutableCommitParams.builder()
                .toBranch(unreachable)
                .commitMetaSerialized(ByteString.copyFromUtf8("commit meta"))
                .addPuts(
                    KeyWithBytes.of(
                        Key.of("foo"),
                        ContentId.of(hello.getId()),
                        payloadForContent(hello),
                        hello.serialized()))
                .build());

    assertAll(
        () ->
            assertThatThrownBy(
                    () -> databaseAdapter.hashOnReference(main, Optional.of(unreachableHead)))
                .isInstanceOf(ReferenceNotFoundException.class)
                .hasMessage(
                    String.format(
                        "Could not find commit '%s' in reference '%s'.",
                        unreachableHead.asString(), main.getName())),
        () ->
            assertThatThrownBy(
                    () -> {
                      OnRefOnly noNo = onRef("hello", "contentId-no-no");
                      databaseAdapter.commit(
                          ImmutableCommitParams.builder()
                              .toBranch(helper)
                              .expectedHead(Optional.of(unreachableHead))
                              .commitMetaSerialized(ByteString.copyFromUtf8("commit meta"))
                              .addPuts(
                                  KeyWithBytes.of(
                                      Key.of("bar"),
                                      ContentId.of(noNo.getId()),
                                      payloadForContent(noNo),
                                      noNo.serialized()))
                              .build());
                    })
                .isInstanceOf(ReferenceNotFoundException.class)
                .hasMessage(
                    String.format(
                        "Could not find commit '%s' in reference '%s'.",
                        unreachableHead.asString(), helper.getName())),
        () ->
            assertThatThrownBy(
                    () ->
                        databaseAdapter.assign(
                            helper,
                            Optional.of(unreachableHead),
                            databaseAdapter.hashOnReference(main, Optional.empty())))
                .isInstanceOf(ReferenceConflictException.class)
                .hasMessage(
                    String.format(
                        "Named-reference '%s' is not at expected hash '%s', but at '%s'.",
                        helper.getName(), unreachableHead.asString(), helperHead.asString())));
  }

  @Test
  void assign() throws Exception {
    BranchName main = BranchName.of("main");
    TagName tag = TagName.of("tag");
    TagName branch = TagName.of("branch");

    databaseAdapter.create(branch, databaseAdapter.hashOnReference(main, Optional.empty()));
    databaseAdapter.create(tag, databaseAdapter.hashOnReference(main, Optional.empty()));

    Hash beginning = databaseAdapter.hashOnReference(main, Optional.empty());

    Hash[] commits = new Hash[3];
    for (int i = 0; i < commits.length; i++) {
      OnRefOnly hello = onRef("hello " + i, "contentId-" + i);
      commits[i] =
          databaseAdapter.commit(
              ImmutableCommitParams.builder()
                  .toBranch(main)
                  .commitMetaSerialized(ByteString.copyFromUtf8("commit meta " + i))
                  .addPuts(
                      KeyWithBytes.of(
                          Key.of("bar", Integer.toString(i)),
                          ContentId.of(hello.getId()),
                          payloadForContent(hello),
                          hello.serialized()))
                  .build());
    }

    Hash expect = beginning;
    for (Hash commit : commits) {
      assertThat(
              Arrays.asList(
                  databaseAdapter.hashOnReference(branch, Optional.empty()),
                  databaseAdapter.hashOnReference(tag, Optional.empty())))
          .containsExactly(expect, expect);

      databaseAdapter.assign(tag, Optional.of(expect), commit);

      databaseAdapter.assign(branch, Optional.of(expect), commit);

      expect = commit;
    }

    assertThat(
            Arrays.asList(
                databaseAdapter.hashOnReference(branch, Optional.empty()),
                databaseAdapter.hashOnReference(tag, Optional.empty())))
        .containsExactly(commits[commits.length - 1], commits[commits.length - 1]);
  }

  @Test
  void recreateDefaultBranch() throws Exception {
    // note: the default branch cannot be deleted through the TreeApi,
    // but the underlying DatabaseAdapter should still support this operation.

    BranchName main = BranchName.of("main");
    Hash mainHead = databaseAdapter.hashOnReference(main, Optional.empty());
    databaseAdapter.delete(main, Optional.of(mainHead));

    assertThatThrownBy(() -> databaseAdapter.hashOnReference(main, Optional.empty()))
        .isInstanceOf(ReferenceNotFoundException.class);

    databaseAdapter.create(main, null);
    databaseAdapter.hashOnReference(main, Optional.empty());
  }

  /**
   * Validates that multiple reference-name segments work, that the split ref-log-heads works over
   * multiple pages.
   */
  @Test
  void manyReferencesAndSplitRefLog() throws Exception {
    IntFunction<NamedRef> refGen =
        i -> {
          StringBuilder sb = new StringBuilder(120);
          sb.append("manyReferencesTest-").append(i).append('-');
          while (sb.length() < 100) {
            sb.append('x');
          }
          String name = sb.toString();
          return (i & 1) == 1 ? TagName.of(name) : BranchName.of(name);
        };

    Map<NamedRef, List<Tuple>> refLogOpsPerRef = new HashMap<>();
    Map<NamedRef, Hash> refHeads = new HashMap<>();

    for (int i = 0; i < 50; i++) {
      NamedRef ref = refGen.apply(i);

      assertThat(databaseAdapter.create(ref, databaseAdapter.noAncestorHash()))
          .isEqualTo(databaseAdapter.noAncestorHash());

      refLogOpsPerRef
          .computeIfAbsent(ref, x -> new ArrayList<>())
          .add(tuple("CREATE_REFERENCE", databaseAdapter.noAncestorHash()));
      refHeads.put(ref, databaseAdapter.noAncestorHash());

      assertThat(databaseAdapter.namedRef(ref.getName(), GetNamedRefsParams.DEFAULT).getNamedRef())
          .isEqualTo(ref);
    }

    // Verify that the HEADs of all references point to the no-ancestor-hash
    try (Stream<ReferenceInfo<ByteString>> refs =
        databaseAdapter.namedRefs(GetNamedRefsParams.DEFAULT)) {
      assertThat(refs.filter(ri -> ri.getNamedRef().getName().startsWith("manyReferencesTest-")))
          .containsExactlyInAnyOrderElementsOf(
              IntStream.range(0, 50)
                  .mapToObj(refGen)
                  .map(ref -> ReferenceInfo.<ByteString>of(databaseAdapter.noAncestorHash(), ref))
                  .collect(Collectors.toList()));
    }

    // add 50 commits to every branch (crossing the number of parents per commit log entry)
    for (int commit = 0; commit < 50; commit++) {
      for (int i = 0; i < 50; i++) {
        NamedRef ref = refGen.apply(i);
        if (ref instanceof BranchName) {
          Hash newHead =
              databaseAdapter.commit(
                  ImmutableCommitParams.builder()
                      .toBranch((BranchName) ref)
                      .commitMetaSerialized(ByteString.copyFromUtf8("foo"))
                      .expectedHead(Optional.of(refHeads.get(ref)))
                      .addPuts(
                          KeyWithBytes.of(
                              Key.of("table", "c" + commit),
                              ContentId.of("c" + commit),
                              payloadForContent(OnRefOnly.ON_REF_ONLY),
                              DefaultStoreWorker.instance()
                                  .toStoreOnReferenceState(
                                      OnRefOnly.newOnRef("c" + commit), att -> {})))
                      .build());
          refHeads.put(ref, newHead);
        }
      }
    }

    // drop every 3rd reference
    for (int i = 2; i < 50; i += 3) {
      NamedRef ref = refGen.apply(i);
      databaseAdapter.delete(ref, Optional.empty());
      assertThatThrownBy(() -> databaseAdapter.namedRef(ref.getName(), GetNamedRefsParams.DEFAULT))
          .isInstanceOf(ReferenceNotFoundException.class);

      refLogOpsPerRef
          .computeIfAbsent(ref, x -> new ArrayList<>())
          .add(tuple("DELETE_REFERENCE", refHeads.get(ref)));
    }

    // Verify HEAD hashes for remaining branches + tags
    try (Stream<ReferenceInfo<ByteString>> refs =
        databaseAdapter.namedRefs(GetNamedRefsParams.DEFAULT)) {
      assertThat(refs.filter(ri -> ri.getNamedRef().getName().startsWith("manyReferencesTest-")))
          .containsExactlyInAnyOrderElementsOf(
              IntStream.range(0, 50)
                  .filter(i -> (i - 2) % 3 != 0)
                  .mapToObj(refGen)
                  .map(
                      ref ->
                          ReferenceInfo.<ByteString>of(
                              refHeads.getOrDefault(ref, databaseAdapter.noAncestorHash()), ref))
                  .collect(Collectors.toList()));
    }

    // Verify that the CREATE_REFERENCE + DROP_REFERENCE + COMMIT reflog entries exist and are in
    // the right order -> DROP_REFERENCE appear before CREATE_REFERENCE.
    try (Stream<RefLog> refLog = databaseAdapter.refLog(null)) {
      refLog
          .filter(l -> l.getRefName().startsWith("manyReferencesTest-"))
          .forEach(
              l -> {
                NamedRef ref =
                    "Branch".equals(l.getRefType())
                        ? BranchName.of(l.getRefName())
                        : TagName.of(l.getRefName());
                List<Tuple> refOps = refLogOpsPerRef.get(ref);
                assertThat(refOps)
                    .describedAs("RefLog operations %s for %s", refOps, l)
                    .isNotNull()
                    .last()
                    .isEqualTo(tuple(l.getOperation(), l.getCommitHash()));
                refOps.remove(refOps.size() - 1);
              });
    }
    assertThat(refLogOpsPerRef).allSatisfy((ref, ops) -> assertThat(ops).isEmpty());
  }
}
