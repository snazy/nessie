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
package org.projectnessie.versioned.storage.versionstore;

import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static org.junit.jupiter.params.provider.Arguments.arguments;
import static org.projectnessie.versioned.storage.common.indexes.StoreKey.key;
import static org.projectnessie.versioned.storage.common.logic.CommitConflict.ConflictType.CONTENT_ID_DIFFERS;
import static org.projectnessie.versioned.storage.common.logic.CommitConflict.ConflictType.KEY_DOES_NOT_EXIST;
import static org.projectnessie.versioned.storage.common.logic.CommitConflict.ConflictType.KEY_EXISTS;
import static org.projectnessie.versioned.storage.common.logic.CommitConflict.ConflictType.PAYLOAD_DIFFERS;
import static org.projectnessie.versioned.storage.common.logic.CommitConflict.ConflictType.VALUE_DIFFERS;
import static org.projectnessie.versioned.storage.common.logic.CommitConflict.commitConflict;
import static org.projectnessie.versioned.storage.common.logic.CreateCommit.newCommitBuilder;
import static org.projectnessie.versioned.storage.common.logic.Logics.commitLogic;
import static org.projectnessie.versioned.storage.common.logic.Logics.referenceLogic;
import static org.projectnessie.versioned.storage.common.objtypes.CommitHeaders.EMPTY_COMMIT_HEADERS;
import static org.projectnessie.versioned.storage.common.persist.ObjId.EMPTY_OBJ_ID;
import static org.projectnessie.versioned.storage.common.persist.ObjId.objIdFromString;
import static org.projectnessie.versioned.storage.common.persist.ObjType.COMMIT;
import static org.projectnessie.versioned.storage.common.persist.Reference.reference;
import static org.projectnessie.versioned.storage.versionstore.RefMapping.NO_ANCESTOR;
import static org.projectnessie.versioned.storage.versionstore.RefMapping.REFS_HEADS;
import static org.projectnessie.versioned.storage.versionstore.RefMapping.REFS_TAGS;
import static org.projectnessie.versioned.storage.versionstore.RefMapping.asBranchName;
import static org.projectnessie.versioned.storage.versionstore.RefMapping.hashNotFound;
import static org.projectnessie.versioned.storage.versionstore.RefMapping.objectNotFound;
import static org.projectnessie.versioned.storage.versionstore.RefMapping.referenceAlreadyExists;
import static org.projectnessie.versioned.storage.versionstore.RefMapping.referenceConflictException;
import static org.projectnessie.versioned.storage.versionstore.RefMapping.referenceNotFound;
import static org.projectnessie.versioned.storage.versionstore.TypeMapping.keyToStoreKey;
import static org.projectnessie.versioned.storage.versionstore.TypeMapping.objIdToHash;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.stream.Stream;
import org.assertj.core.api.SoftAssertions;
import org.assertj.core.api.junit.jupiter.InjectSoftAssertions;
import org.assertj.core.api.junit.jupiter.SoftAssertionsExtension;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.projectnessie.model.ContentKey;
import org.projectnessie.versioned.BranchName;
import org.projectnessie.versioned.DetachedRef;
import org.projectnessie.versioned.Hash;
import org.projectnessie.versioned.ReferenceAlreadyExistsException;
import org.projectnessie.versioned.ReferenceConflictException;
import org.projectnessie.versioned.ReferenceNotFoundException;
import org.projectnessie.versioned.TagName;
import org.projectnessie.versioned.storage.common.exceptions.CommitConflictException;
import org.projectnessie.versioned.storage.common.exceptions.ObjNotFoundException;
import org.projectnessie.versioned.storage.common.logic.ReferenceLogic;
import org.projectnessie.versioned.storage.common.objtypes.CommitObj;
import org.projectnessie.versioned.storage.common.persist.ObjId;
import org.projectnessie.versioned.storage.common.persist.Persist;
import org.projectnessie.versioned.storage.common.persist.Reference;
import org.projectnessie.versioned.storage.testextension.NessiePersist;
import org.projectnessie.versioned.storage.testextension.PersistExtension;

@ExtendWith({PersistExtension.class, SoftAssertionsExtension.class})
public class TestRefMapping {
  @NessiePersist protected static Persist persist;

  @InjectSoftAssertions protected SoftAssertions soft;

  static Stream<Arguments> exceptions() {
    return Stream.of(
        arguments(
            referenceNotFound("foo"),
            ReferenceNotFoundException.class,
            "Named reference 'foo' not found"),
        arguments(
            referenceNotFound(BranchName.of("foo")),
            ReferenceNotFoundException.class,
            "Named reference 'foo' not found"),
        arguments(
            referenceAlreadyExists(BranchName.of("foo")),
            ReferenceAlreadyExistsException.class,
            "Named reference 'foo' already exists."),
        arguments(
            hashNotFound(BranchName.of("foo"), Hash.of("1234")),
            ReferenceNotFoundException.class,
            "Could not find commit '1234' in reference 'foo'."),
        arguments(
            hashNotFound(Hash.of("1234")),
            ReferenceNotFoundException.class,
            "Commit '1234' not found"),
        arguments(
            objectNotFound(objIdFromString("1234")),
            ReferenceNotFoundException.class,
            "Commit '1234' not found"),
        arguments(
            objectNotFound(new ObjNotFoundException(objIdFromString("1234"))),
            ReferenceNotFoundException.class,
            "Commit '1234' not found"),
        arguments(
            objectNotFound(
                new ObjNotFoundException(asList(objIdFromString("1234"), objIdFromString("5678")))),
            ReferenceNotFoundException.class,
            "Could not find objects '1234', '5678'."),
        arguments(
            referenceNotFound(new ObjNotFoundException(singletonList(objIdFromString("1234")))),
            ReferenceNotFoundException.class,
            "Commit '1234' not found"),
        arguments(
            referenceNotFound(
                new ObjNotFoundException(asList(objIdFromString("1234"), objIdFromString("5678")))),
            ReferenceNotFoundException.class,
            "Could not find commits '1234', '5678'."),
        arguments(
            referenceConflictException(
                new CommitConflictException(
                    singletonList(
                        commitConflict(
                            keyToStoreKey(ContentKey.of("aaa", "foo")),
                            KEY_DOES_NOT_EXIST,
                            null)))),
            ReferenceConflictException.class,
            "There are conflicts that prevent committing the provided operations: key 'aaa.foo' does not exist."),
        arguments(
            referenceConflictException(
                new CommitConflictException(
                    singletonList(commitConflict(key("aaa", "foo"), KEY_DOES_NOT_EXIST, null)))),
            ReferenceConflictException.class,
            "There are conflicts that prevent committing the provided operations: store-key 'aaa/foo' does not exist."),
        arguments(
            referenceConflictException(
                new CommitConflictException(
                    asList(
                        commitConflict(key("aaa", "foo"), KEY_EXISTS, null),
                        commitConflict(key("bbb", "foo"), KEY_DOES_NOT_EXIST, null),
                        commitConflict(key("ccc", "foo"), PAYLOAD_DIFFERS, null),
                        commitConflict(key("ddd", "foo"), CONTENT_ID_DIFFERS, null),
                        commitConflict(key("eee", "foo"), VALUE_DIFFERS, null)))),
            ReferenceConflictException.class,
            "There are conflicts that prevent committing the provided operations: "
                + "store-key 'aaa/foo' already exists, "
                + "store-key 'bbb/foo' does not exist, "
                + "payload of existing and expected content for store-key 'ccc/foo' are different, "
                + "content IDs of existing and expected content for store-key 'ddd/foo' are different, "
                + "values of existing and expected content for store-key 'eee/foo' are different."),
        arguments(
            referenceConflictException(
                new CommitConflictException(
                    asList(
                        commitConflict(
                            keyToStoreKey(ContentKey.of("aaa", "foo")), KEY_EXISTS, null),
                        commitConflict(
                            keyToStoreKey(ContentKey.of("bbb", "foo")), KEY_DOES_NOT_EXIST, null),
                        commitConflict(
                            keyToStoreKey(ContentKey.of("ccc", "foo")), PAYLOAD_DIFFERS, null),
                        commitConflict(
                            keyToStoreKey(ContentKey.of("ddd", "foo")), CONTENT_ID_DIFFERS, null),
                        commitConflict(
                            keyToStoreKey(ContentKey.of("eee", "foo")), VALUE_DIFFERS, null)))),
            ReferenceConflictException.class,
            "There are conflicts that prevent committing the provided operations: "
                + "key 'aaa.foo' already exists, "
                + "key 'bbb.foo' does not exist, "
                + "payload of existing and expected content for key 'ccc.foo' are different, "
                + "content IDs of existing and expected content for key 'ddd.foo' are different, "
                + "values of existing and expected content for key 'eee.foo' are different."),
        arguments(
            referenceConflictException(
                BranchName.of("foo"), Hash.of("1234"), objIdFromString("5678")),
            ReferenceConflictException.class,
            "Named-reference 'foo' is not at expected hash '1234', but at '5678'."));
  }

  @ParameterizedTest
  @MethodSource("exceptions")
  public void exceptions(Exception e, Class<? extends Exception> type, String message) {
    soft.assertThat(e).isInstanceOf(type).hasMessage(message);
  }

  @Test
  public void verifyExpectedHash() {
    soft.assertThatCode(
            () ->
                RefMapping.verifyExpectedHash(
                    Hash.of("1234"), BranchName.of("foo-branch"), Optional.of(Hash.of("1234"))))
        .doesNotThrowAnyException();
    soft.assertThatCode(
            () ->
                RefMapping.verifyExpectedHash(
                    Hash.of("1234"), BranchName.of("foo-branch"), Optional.empty()))
        .doesNotThrowAnyException();
    soft.assertThatThrownBy(
            () ->
                RefMapping.verifyExpectedHash(
                    Hash.of("1234"), BranchName.of("foo-branch"), Optional.of(Hash.of("5678"))))
        .isInstanceOf(ReferenceConflictException.class)
        .hasMessage("Named-reference 'foo-branch' is not at expected hash '5678', but at '1234'.");
  }

  @Test
  public void referenceToNamedRef() {
    soft.assertThat(RefMapping.referenceToNamedRef(REFS_HEADS + "main"))
        .isEqualTo(BranchName.of("main"));
    soft.assertThat(RefMapping.referenceToNamedRef(REFS_HEADS + "foo/bar"))
        .isEqualTo(BranchName.of("foo/bar"));
    soft.assertThat(RefMapping.referenceToNamedRef(REFS_TAGS + "tag")).isEqualTo(TagName.of("tag"));
    soft.assertThat(RefMapping.referenceToNamedRef(REFS_TAGS + "foo/bar"))
        .isEqualTo(TagName.of("foo/bar"));
    soft.assertThatIllegalArgumentException()
        .isThrownBy(() -> RefMapping.referenceToNamedRef(REFS_HEADS));
    soft.assertThatIllegalArgumentException()
        .isThrownBy(() -> RefMapping.referenceToNamedRef("refs/heads"));
    soft.assertThatIllegalArgumentException()
        .isThrownBy(() -> RefMapping.referenceToNamedRef(REFS_TAGS));
    soft.assertThatIllegalArgumentException()
        .isThrownBy(() -> RefMapping.referenceToNamedRef("refs/tags"));
    soft.assertThatIllegalArgumentException().isThrownBy(() -> RefMapping.referenceToNamedRef(""));
    soft.assertThatIllegalArgumentException()
        .isThrownBy(() -> RefMapping.referenceToNamedRef("fooo"));
    soft.assertThatIllegalArgumentException()
        .isThrownBy(() -> RefMapping.referenceToNamedRef("refs/blah"));
    soft.assertThatIllegalArgumentException()
        .isThrownBy(() -> RefMapping.referenceToNamedRef("int/repo"));

    soft.assertThat(
            RefMapping.referenceToNamedRef(reference(REFS_HEADS + "main", EMPTY_OBJ_ID, false)))
        .isEqualTo(BranchName.of("main"));
    soft.assertThat(
            RefMapping.referenceToNamedRef(reference(REFS_TAGS + "tag", EMPTY_OBJ_ID, false)))
        .isEqualTo(TagName.of("tag"));
    soft.assertThatIllegalArgumentException()
        .isThrownBy(() -> RefMapping.referenceToNamedRef(reference("", EMPTY_OBJ_ID, false)));
  }

  @Test
  public void namedRefToRefName() {
    soft.assertThat(RefMapping.namedRefToRefName(BranchName.of("main")))
        .isEqualTo(REFS_HEADS + "main");
    soft.assertThat(RefMapping.namedRefToRefName(BranchName.of("foo/bar")))
        .isEqualTo(REFS_HEADS + "foo/bar");
    soft.assertThat(RefMapping.namedRefToRefName(TagName.of("tag"))).isEqualTo(REFS_TAGS + "tag");
    soft.assertThat(RefMapping.namedRefToRefName(TagName.of("foo/bar")))
        .isEqualTo(REFS_TAGS + "foo/bar");
    soft.assertThatIllegalArgumentException()
        .isThrownBy(() -> RefMapping.namedRefToRefName(DetachedRef.INSTANCE));
  }

  @Test
  public void resolveRefs() throws Exception {
    ReferenceLogic referenceLogic = referenceLogic(persist);
    RefMapping refMapping = new RefMapping(persist);

    ObjId commitId = generateCommit(EMPTY_OBJ_ID, "foo", 42);

    Reference branch = referenceLogic.createReference(REFS_HEADS + "branch", commitId);
    Reference tag = referenceLogic.createReference(REFS_TAGS + "tag", commitId);
    Reference emptyBranch = referenceLogic.createReference(REFS_HEADS + "empty", EMPTY_OBJ_ID);
    Reference emptyTag = referenceLogic.createReference(REFS_TAGS + "empty", EMPTY_OBJ_ID);
    Reference notThere =
        referenceLogic.createReference(REFS_TAGS + "not-there", objIdFromString("00001111"));

    soft.assertThat(refMapping.resolveNamedRef("branch")).isEqualTo(branch);
    soft.assertThat(refMapping.resolveNamedRef("tag")).isEqualTo(tag);
    soft.assertThat(refMapping.resolveNamedRef(BranchName.of("branch"))).isEqualTo(branch);
    soft.assertThat(refMapping.resolveNamedRef(TagName.of("tag"))).isEqualTo(tag);

    soft.assertThat(refMapping.resolveNamedRef("empty")).isEqualTo(emptyBranch);
    soft.assertThat(refMapping.resolveNamedRef(BranchName.of("empty"))).isEqualTo(emptyBranch);
    soft.assertThat(refMapping.resolveNamedRef(TagName.of("empty"))).isEqualTo(emptyTag);

    soft.assertThatThrownBy(() -> refMapping.resolveNamedRef("does-not-exist"))
        .isInstanceOf(ReferenceNotFoundException.class);

    soft.assertThat(refMapping.resolveNamedRefHead(branch))
        .isEqualTo(persist.fetchTypedObj(commitId, COMMIT, CommitObj.class));
    soft.assertThat(refMapping.resolveNamedRefHead(tag))
        .isEqualTo(persist.fetchTypedObj(commitId, COMMIT, CommitObj.class));

    soft.assertThat(refMapping.resolveNamedRefHead(BranchName.of("branch")))
        .isEqualTo(persist.fetchTypedObj(commitId, COMMIT, CommitObj.class));
    soft.assertThat(refMapping.resolveNamedRefHead(TagName.of("tag")))
        .isEqualTo(persist.fetchTypedObj(commitId, COMMIT, CommitObj.class));

    soft.assertThat(refMapping.resolveRefHead(BranchName.of("branch")))
        .isEqualTo(persist.fetchTypedObj(commitId, COMMIT, CommitObj.class));
    soft.assertThat(refMapping.resolveRefHead(TagName.of("tag")))
        .isEqualTo(persist.fetchTypedObj(commitId, COMMIT, CommitObj.class));
    soft.assertThat(refMapping.resolveRefHead(Hash.of(commitId.toString())))
        .isEqualTo(persist.fetchTypedObj(commitId, COMMIT, CommitObj.class));

    soft.assertThat(refMapping.resolveNamedRefHead(emptyBranch)).isNull();
    soft.assertThat(refMapping.resolveNamedRefHead(emptyTag)).isNull();
    soft.assertThatThrownBy(() -> refMapping.resolveNamedRefHead(notThere))
        .isInstanceOf(ReferenceNotFoundException.class);

    soft.assertThat(refMapping.resolveNamedRefHead(BranchName.of("empty"))).isNull();
    soft.assertThat(refMapping.resolveNamedRefHead(TagName.of("empty"))).isNull();
    soft.assertThatThrownBy(() -> refMapping.resolveNamedRefHead(BranchName.of("not-there")))
        .isInstanceOf(ReferenceNotFoundException.class);

    soft.assertThat(refMapping.resolveRefHead(BranchName.of("empty"))).isNull();
    soft.assertThat(refMapping.resolveRefHead(TagName.of("empty"))).isNull();
    soft.assertThat(refMapping.resolveRefHead(Hash.of(EMPTY_OBJ_ID.toString()))).isNull();
    soft.assertThatThrownBy(() -> refMapping.resolveRefHead(BranchName.of("not-there")))
        .isInstanceOf(ReferenceNotFoundException.class);

    soft.assertThatIllegalArgumentException()
        .isThrownBy(() -> refMapping.resolveRefHead(DetachedRef.INSTANCE));
    soft.assertThatThrownBy(() -> refMapping.resolveRefHead(Hash.of("00001111")))
        .isInstanceOf(ReferenceNotFoundException.class);
  }

  @Test
  public void commitInChain() throws Exception {
    ReferenceLogic referenceLogic = referenceLogic(persist);
    RefMapping refMapping = new RefMapping(persist);

    List<ObjId> commits1 = generateCommits("foo");
    ObjId commits1head = commits1.get(commits1.size() - 1);
    List<ObjId> commits2 = generateCommits("bar");

    referenceLogic.createReference(asBranchName("branch"), commits1head);
    for (ObjId testId : commits1) {
      CommitObj commit = refMapping.commitInChain(commits1head, testId);
      soft.assertThat(commit).isNotNull();
      soft.assertThat(
              refMapping.commitInChain(
                  BranchName.of("branch"), commit, Optional.of(objIdToHash(testId))))
          .isNotNull();
      soft.assertThat(refMapping.commitInChain(BranchName.of("branch"), commit, Optional.empty()))
          .isSameAs(commit);
      soft.assertThat(
              refMapping.commitInChain(BranchName.of("branch"), commit, Optional.of(NO_ANCESTOR)))
          .isNull();
    }
    for (ObjId testId : commits2) {
      soft.assertThat(refMapping.commitInChain(commits1head, testId)).isNull();
    }
  }

  private List<ObjId> generateCommits(String msg) throws Exception {
    ObjId head = EMPTY_OBJ_ID;
    List<ObjId> r = new ArrayList<>();
    for (int i = 0; i < 10; i++) {
      head = generateCommit(head, msg, i);
      r.add(head);
    }
    return r;
  }

  private ObjId generateCommit(ObjId head, String msg, int i) throws Exception {
    return commitLogic(persist)
        .doCommit(
            newCommitBuilder()
                .parentCommitId(head)
                .message("commit " + msg + " " + i)
                .headers(EMPTY_COMMIT_HEADERS)
                .build(),
            emptyList());
  }
}
