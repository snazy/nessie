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
package org.projectnessie.versioned.tests;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Optional;
import java.util.Random;
import java.util.function.IntFunction;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.hamcrest.Matchers;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.projectnessie.versioned.BranchName;
import org.projectnessie.versioned.Delete;
import org.projectnessie.versioned.Diff;
import org.projectnessie.versioned.Hash;
import org.projectnessie.versioned.ImmutableBranchName;
import org.projectnessie.versioned.ImmutableKey;
import org.projectnessie.versioned.ImmutablePut;
import org.projectnessie.versioned.ImmutableTagName;
import org.projectnessie.versioned.Key;
import org.projectnessie.versioned.NamedRef;
import org.projectnessie.versioned.Operation;
import org.projectnessie.versioned.Put;
import org.projectnessie.versioned.Ref;
import org.projectnessie.versioned.ReferenceAlreadyExistsException;
import org.projectnessie.versioned.ReferenceConflictException;
import org.projectnessie.versioned.ReferenceNotFoundException;
import org.projectnessie.versioned.StringSerializer;
import org.projectnessie.versioned.TagName;
import org.projectnessie.versioned.Unchanged;
import org.projectnessie.versioned.VersionStore;
import org.projectnessie.versioned.VersionStoreException;
import org.projectnessie.versioned.WithHash;
import org.projectnessie.versioned.WithType;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;

/**
 * Base class used for integration tests against {@link VersionStore} implementations.
 * Integration tests for {@code TieredVersionStore} implementations must extend
 * {@code org.projectnessie.versioned.impl.AbstractITTieredVersionStore}.
 */
public abstract class AbstractITVersionStore {

  protected abstract VersionStore<String, String, StringSerializer.TestEnum> versionStore();

  /**
   * Use case simulation: single branch, multiple users, each user updating a separate table.
   */
  @Test
  void singleBranchManyUsersDistinctTables() throws Exception {
    singleBranchTest("singleBranchManyUsersDistinctTables", user -> String.format("user-table-%d", user), false);
  }

  /**
   * Use case simulation: single branch, multiple users, all users updating a single table.
   */
  @Test
  void singleBranchManyUsersSingleTable() throws Exception {
    singleBranchTest("singleBranchManyUsersSingleTable", user -> "single-table", true);
  }

  private void singleBranchTest(String branchName, IntFunction<String> tableNameGen,
      boolean allowInconsistentValueException) throws Exception {
    BranchName branch = BranchName.of(branchName);

    int numUsers = 5;
    int numCommits = 50;

    Hash[] hashesKnownByUser = new Hash[numUsers];
    Hash createHash = versionStore().create(branch, Optional.empty());
    Arrays.fill(hashesKnownByUser, createHash);

    List<String> expectedValues = new ArrayList<>();
    for (int commitNum = 0; commitNum < numCommits; commitNum++) {
      for (int user = 0; user < numUsers; user++) {
        Hash hashKnownByUser = hashesKnownByUser[user];

        String msg = String.format("user %03d/commit %03d", user, commitNum);
        expectedValues.add(msg);
        String value = String.format("data_file_%03d_%03d", user, commitNum);
        Put<String> put = Put.of(Key.of(tableNameGen.apply(user)), value);

        Hash commitHash;
        try {
          commitHash = versionStore().commit(branch, Optional.of(hashKnownByUser), msg, ImmutableList.of(put));
        } catch (ReferenceConflictException inconsistentValueException) {
          if (allowInconsistentValueException) {
            hashKnownByUser = versionStore().toHash(branch);
            commitHash = versionStore().commit(branch, Optional.of(hashKnownByUser), msg, ImmutableList.of(put));
          } else {
            throw inconsistentValueException;
          }
        }

        assertNotEquals(hashKnownByUser, commitHash);

        hashesKnownByUser[user] = commitHash;
      }
    }

    // Verify that all commits are there and that the order of the commits is correct
    List<String> committedValues = versionStore().getCommits(branch)
        .map(WithHash::getValue).collect(Collectors.toList());
    Collections.reverse(expectedValues);
    assertEquals(expectedValues, committedValues);
  }

  /*
   * Test:
   * - Create a branch with no hash assigned to it
   * - check that a hash is returned by toHash
   * - check the branch is returned by getNamedRefs
   * - check that no commits are returned using getCommits
   * - check the branch cannot be created
   * - check the branch can be deleted
   */
  @Test
  public void createAndDeleteBranch() throws Exception {
    final BranchName branch = BranchName.of("foo");
    versionStore().create(branch, Optional.empty());
    final Hash hash = versionStore().toHash(branch);
    assertThat(hash, is(notNullValue()));

    final BranchName anotherBranch = BranchName.of("bar");
    final Hash createHash = versionStore().create(anotherBranch, Optional.of(hash));
    final Hash commitHash = commit("Some Commit").toBranch(anotherBranch);
    assertNotEquals(createHash, commitHash);

    final BranchName anotherAnotherBranch = BranchName.of("baz");
    final Hash otherCreateHash = versionStore().create(anotherAnotherBranch, Optional.of(commitHash));
    assertEquals(commitHash, otherCreateHash);

    List<WithHash<NamedRef>> namedRefs;
    try (Stream<WithHash<NamedRef>> str = versionStore().getNamedRefs()) {
      namedRefs = str.collect(Collectors.toList());
    }
    assertThat(namedRefs, containsInAnyOrder(
        WithHash.of(hash, branch),
        WithHash.of(commitHash, anotherBranch),
        WithHash.of(commitHash, anotherAnotherBranch)
    ));

    assertThat(versionStore().getCommits(branch).count(), is(0L));
    assertThat(versionStore().getCommits(anotherBranch).count(), is(1L));
    assertThat(versionStore().getCommits(anotherAnotherBranch).count(), is(1L));
    assertThat(versionStore().getCommits(hash).count(), is(0L)); // empty commit should not be listed
    assertThat(versionStore().getCommits(commitHash).count(), is(1L)); // empty commit should not be listed

    assertThrows(ReferenceAlreadyExistsException.class, () -> versionStore().create(branch, Optional.empty()));
    assertThrows(ReferenceAlreadyExistsException.class, () -> versionStore().create(branch, Optional.of(hash)));

    versionStore().delete(branch, Optional.of(hash));
    assertThrows(ReferenceNotFoundException.class, () -> versionStore().toHash(branch));
    try (Stream<WithHash<NamedRef>> str = versionStore().getNamedRefs()) {
      assertThat(str.count(), is(2L)); // bar + baz
    }
    assertThrows(ReferenceNotFoundException.class, () -> versionStore().delete(branch, Optional.of(hash)));
  }

  @Test
  public void commitLogPaging() throws Exception {
    BranchName branch = BranchName.of("commitLogPaging");
    Hash createHash = versionStore().create(branch, Optional.empty());

    int commits = 95; // this should be enough
    Hash[] commitHashes = new Hash[commits];
    List<String> messages = new ArrayList<>(commits);
    for (int i = 0; i < commits; i++) {
      String msg = String.format("commit#%05d", i);
      messages.add(msg);
      commitHashes[i] = versionStore().commit(branch, Optional.of(i == 0 ? createHash : commitHashes[i - 1]),
          msg, ImmutableList.of(Put.of(Key.of("table"), String.format("value#%05d", i))));
    }
    Collections.reverse(messages);

    Stream<WithHash<String>> justTwo = versionStore().getCommits(branch).limit(2);
    assertEquals(messages.subList(0, 2), justTwo.map(WithHash::getValue).collect(Collectors.toList()));
    Stream<WithHash<String>> justTen = versionStore().getCommits(branch).limit(10);
    assertEquals(messages.subList(0, 10), justTen.map(WithHash::getValue).collect(Collectors.toList()));

    int pageSize = 10;

    // Test parameter sanity check. Want the last page to be smaller than the page-size.
    assertNotEquals(0, commits % (pageSize - 1));

    Hash lastHash = null;
    for (int offset = 0; ; ) {
      List<WithHash<String>> logPage = versionStore().getCommits(lastHash == null ? branch : lastHash)
          .limit(pageSize).collect(Collectors.toList());

      assertEquals(
          messages.subList(offset, Math.min(offset + pageSize, commits)),
          logPage.stream().map(WithHash::getValue).collect(Collectors.toList()));

      lastHash = logPage.get(logPage.size() - 1).getHash();

      offset += pageSize - 1;
      if (offset >= commits) {
        // The "next after last page" should always return just a single commit, that's basically
        // the "end of commit-log"-condition.
        logPage = versionStore().getCommits(lastHash).limit(pageSize).collect(Collectors.toList());
        assertEquals(
            Collections.singletonList(messages.get(commits - 1)),
            logPage.stream().map(WithHash::getValue).collect(Collectors.toList()));
        break;
      }
    }
  }

  /*
   * Test:
   * - Create a branch with no hash assigned to it
   * - add a commit to the branch
   * - create a tag for the initial hash
   * - create another tag for the hash after the commit
   * - check that cannot create existing tags, or tag with no assigned hash
   * - check that a hash is returned by toHash
   * - check the tags are returned by getNamedRefs
   * - check that expected commits are returned by getCommits
   * - check the branch can be deleted
   */
  @Test
  public void createAndDeleteTag() throws Exception {
    final BranchName branch = BranchName.of("foo");
    versionStore().create(branch, Optional.empty());

    final Hash initialHash = versionStore().toHash(branch);
    final Hash commitHash = commit("Some commit").toBranch(branch);

    final TagName tag = TagName.of("tag");
    versionStore().create(tag, Optional.of(initialHash));

    final TagName anotherTag = TagName.of("another-tag");
    versionStore().create(anotherTag, Optional.of(commitHash));

    assertThrows(ReferenceAlreadyExistsException.class, () -> versionStore().create(tag, Optional.of(initialHash)));
    assertThrows(IllegalArgumentException.class, () -> versionStore().create(tag,  Optional.empty()));

    assertThat(versionStore().toHash(tag), is(initialHash));
    assertThat(versionStore().toHash(anotherTag), is(commitHash));

    List<WithHash<NamedRef>> namedRefs;
    try (Stream<WithHash<NamedRef>> str = versionStore().getNamedRefs()) {
      namedRefs = str.collect(Collectors.toList());
    }
    assertThat(namedRefs, containsInAnyOrder(
        WithHash.of(commitHash, branch),
        WithHash.of(initialHash, tag),
        WithHash.of(commitHash, anotherTag)));

    assertThat(versionStore().getCommits(tag).count(), is(0L));
    assertThat(versionStore().getCommits(initialHash).count(), is(0L)); // empty commit should not be listed

    assertThat(versionStore().getCommits(anotherTag).count(), is(1L));
    assertThat(versionStore().getCommits(commitHash).count(), is(1L)); // empty commit should not be listed

    versionStore().delete(tag, Optional.of(initialHash));
    assertThrows(ReferenceNotFoundException.class, () -> versionStore().toHash(tag));
    try (Stream<WithHash<NamedRef>> str = versionStore().getNamedRefs()) {
      assertThat(str.count(), is(2L)); // foo + another-tag
    }
    assertThrows(ReferenceNotFoundException.class, () -> versionStore().delete(tag, Optional.of(initialHash)));
  }

  /*
   * Test:
   * - Create a new branch
   * - Add a commit to it
   * - Check that another commit with no operations can be added with the initial hash
   * - Check the commit can be listed
   * - Check that the commit can be deleted
   */
  @Test
  public void commitToBranch() throws Exception {
    final BranchName branch = BranchName.of("foo");

    final Hash createHash = versionStore().create(branch, Optional.empty());
    final Hash initialHash = versionStore().toHash(branch);
    assertEquals(createHash, initialHash);

    final Hash commitHash0 = versionStore().commit(branch, Optional.of(initialHash), "Some commit", Collections.emptyList());
    final Hash commitHash = versionStore().toHash(branch);
    assertEquals(commitHash, commitHash0);

    assertThat(commitHash, is(Matchers.not(initialHash)));
    versionStore().commit(branch, Optional.of(initialHash), "Another commit", Collections.emptyList());
    final Hash anotherCommitHash = versionStore().toHash(branch);

    assertThat(versionStore().getCommits(branch).collect(Collectors.toList()), contains(
        WithHash.of(anotherCommitHash, "Another commit"),
        WithHash.of(commitHash, "Some commit")
    ));
    assertThat(versionStore().getCommits(commitHash).collect(Collectors.toList()), contains(WithHash.of(commitHash, "Some commit")));

    assertThrows(ReferenceConflictException.class, () -> versionStore().delete(branch, Optional.of(initialHash)));
    versionStore().delete(branch, Optional.of(anotherCommitHash));
    assertThrows(ReferenceNotFoundException.class, () -> versionStore().toHash(branch));
    try (Stream<WithHash<NamedRef>> str = versionStore().getNamedRefs()) {
      assertThat(str.count(), is(0L));
    }
    assertThrows(ReferenceNotFoundException.class, () -> versionStore().delete(branch, Optional.of(commitHash)));
  }

  /*
   * Test:
   * - Create a new branch
   * - Add 3 commits in succession with no conflicts to it with put and delete operations
   * - Check commit metadata
   * - Check keys for each commit hash
   * - Check values for each commit hash
   */
  @Test
  public void commitSomeOperations() throws Exception {
    final BranchName branch = BranchName.of("foo");

    versionStore().create(branch, Optional.empty());

    final Hash initialCommit = commit("Initial Commit")
        .put("t1", "v1_1")
        .put("t2", "v2_1")
        .put("t3", "v3_1")
        .toBranch(branch);

    final Hash secondCommit = commit("Second Commit")
        .put("t1", "v1_2")
        .delete("t2")
        .delete("t3")
        .put("t4", "v4_1")
        .toBranch(branch);

    final Hash thirdCommit = commit("Third Commit")
        .put("t2", "v2_2")
        .unchanged("t4")
        .toBranch(branch);

    assertThat(versionStore().getCommits(branch).collect(Collectors.toList()), contains(
        WithHash.of(thirdCommit, "Third Commit"),
        WithHash.of(secondCommit, "Second Commit"),
        WithHash.of(initialCommit, "Initial Commit")
        ));

    assertThat(versionStore().getKeys(branch).map(WithType::getValue).collect(Collectors.toList()), containsInAnyOrder(
        Key.of("t1"),
        Key.of("t2"),
        Key.of("t4")
        ));

    assertThat(versionStore().getKeys(secondCommit).map(WithType::getValue).collect(Collectors.toList()), containsInAnyOrder(
        Key.of("t1"),
        Key.of("t4")
        ));

    assertThat(versionStore().getKeys(initialCommit).map(WithType::getValue).collect(Collectors.toList()), containsInAnyOrder(
        Key.of("t1"),
        Key.of("t2"),
        Key.of("t3")
        ));

    assertThat(
        versionStore().getValues(branch, Arrays.asList(Key.of("t1"), Key.of("t2"), Key.of("t3"), Key.of("t4"))),
        contains(
            Optional.of("v1_2"),
            Optional.of("v2_2"),
            Optional.empty(),
            Optional.of("v4_1")
        ));

    assertThat(versionStore().getValues(secondCommit, Arrays.asList(Key.of("t1"), Key.of("t2"), Key.of("t3"), Key.of("t4"))),
        contains(
            Optional.of("v1_2"),
            Optional.empty(),
            Optional.empty(),
            Optional.of("v4_1")
        ));

    assertThat(versionStore().getValues(initialCommit, Arrays.asList(Key.of("t1"), Key.of("t2"), Key.of("t3"), Key.of("t4"))),
        contains(
            Optional.of("v1_1"),
            Optional.of("v2_1"),
            Optional.of("v3_1"),
            Optional.empty()
        ));

    assertThat(versionStore().getValue(branch, Key.of("t1")), is("v1_2"));
    assertThat(versionStore().getValue(branch, Key.of("t2")), is("v2_2"));
    assertThat(versionStore().getValue(branch, Key.of("t3")), is(nullValue()));
    assertThat(versionStore().getValue(branch, Key.of("t4")), is("v4_1"));

    assertThat(versionStore().getValue(secondCommit, Key.of("t1")), is("v1_2"));
    assertThat(versionStore().getValue(secondCommit, Key.of("t2")), is(nullValue()));
    assertThat(versionStore().getValue(secondCommit, Key.of("t3")), is(nullValue()));
    assertThat(versionStore().getValue(secondCommit, Key.of("t4")), is("v4_1"));

    assertThat(versionStore().getValue(initialCommit, Key.of("t1")), is("v1_1"));
    assertThat(versionStore().getValue(initialCommit, Key.of("t2")), is("v2_1"));
    assertThat(versionStore().getValue(initialCommit, Key.of("t3")), is("v3_1"));
    assertThat(versionStore().getValue(initialCommit, Key.of("t4")), is(nullValue()));
  }

  /*
   * Test:
   * - Create a new branch
   * - Add a commit for 3 keys
   * - Add a commit based on initial commit for first key
   * - Add a commit based on initial commit for second key
   * - Add a commit based on initial commit for third  key
   * - Check commit metadata
   * - Check keys for each commit hash
   * - Check values for each commit hash
   */
  @Test
  public void commitNonConflictingOperations() throws Exception {
    final BranchName branch = BranchName.of("foo");

    versionStore().create(branch, Optional.empty());

    final Hash initialCommit = commit("Initial Commit")
        .put("t1", "v1_1")
        .put("t2", "v2_1")
        .put("t3", "v3_1")
        .toBranch(branch);

    final Hash t1Commit = commit("T1 Commit").fromReference(initialCommit).put("t1", "v1_2").toBranch(branch);
    final Hash t2Commit = commit("T2 Commit").fromReference(initialCommit).delete("t2").toBranch(branch);
    final Hash t3Commit = commit("T3 Commit").fromReference(initialCommit).unchanged("t3").toBranch(branch);
    final Hash extraCommit = commit("Extra Commit").fromReference(t1Commit).put("t1", "v1_3").put("t3", "v3_2").toBranch(branch);
    final Hash newT2Commit = commit("New T2 Commit").fromReference(t2Commit).put("t2", "new_v2_1").toBranch(branch);

    assertThat(versionStore().getCommits(branch).collect(Collectors.toList()), contains(
        WithHash.of(newT2Commit, "New T2 Commit"),
        WithHash.of(extraCommit, "Extra Commit"),
        WithHash.of(t3Commit, "T3 Commit"),
        WithHash.of(t2Commit, "T2 Commit"),
        WithHash.of(t1Commit, "T1 Commit"),
        WithHash.of(initialCommit, "Initial Commit")
        ));

    assertThat(versionStore().getKeys(branch).map(WithType::getValue).collect(Collectors.toList()), containsInAnyOrder(
        Key.of("t1"),
        Key.of("t2"),
        Key.of("t3")
        ));

    assertThat(versionStore().getValues(branch, Arrays.asList(Key.of("t1"), Key.of("t2"), Key.of("t3"))),
        contains(
            Optional.of("v1_3"),
            Optional.of("new_v2_1"),
            Optional.of("v3_2")
        ));

    assertThat(versionStore().getValues(newT2Commit, Arrays.asList(Key.of("t1"), Key.of("t2"), Key.of("t3"))),
        contains(
            Optional.of("v1_3"),
            Optional.of("new_v2_1"),
            Optional.of("v3_2")
        ));

    assertThat(versionStore().getValues(extraCommit, Arrays.asList(Key.of("t1"), Key.of("t2"), Key.of("t3"))),
        contains(
            Optional.of("v1_3"),
            Optional.empty(),
            Optional.of("v3_2")
        ));

    assertThat(versionStore().getValues(t3Commit, Arrays.asList(Key.of("t1"), Key.of("t2"), Key.of("t3"))),
        contains(
            Optional.of("v1_2"),
            Optional.empty(),
            Optional.of("v3_1")
        ));

    assertThat(versionStore().getValues(t2Commit, Arrays.asList(Key.of("t1"), Key.of("t2"), Key.of("t3"))),
        contains(
            Optional.of("v1_2"),
            Optional.empty(),
            Optional.of("v3_1")
        ));

    assertThat(versionStore().getValues(t1Commit, Arrays.asList(Key.of("t1"), Key.of("t2"), Key.of("t3"))),
        contains(
            Optional.of("v1_2"),
            Optional.of("v2_1"),
            Optional.of("v3_1")
        ));
  }

  /*
   * Test:
   * - Create a new branch
   * - Add a commit to create 2 keys
   * - Add a second commit to delete one key and add a new one
   * - Check that put operations against 1st commit for the 3 keys fail
   * - Check that delete operations against 1st commit for the 3 keys fail
   * - Check that unchanged operations against 1st commit for the 3 keys fail
   * - Check that branch state hasn't changed
   */
  @Test
  public void commitConflictingOperations() throws Exception {
    final BranchName branch = BranchName.of("foo");

    versionStore().create(branch, Optional.empty());

    final Hash initialCommit = commit("Initial Commit")
        .put("t1", "v1_1")
        .put("t2", "v2_1")
        .toBranch(branch);

    final Hash secondCommit = commit("Second Commit")
        .put("t1", "v1_2")
        .delete("t2")
        .put("t3", "v3_1")
        .toBranch(branch);

    assertThrows(ReferenceConflictException.class,
        () -> commit("Conflicting Commit").fromReference(initialCommit).put("t1", "v1_3").toBranch(branch));
    assertThrows(ReferenceConflictException.class,
        () -> commit("Conflicting Commit").fromReference(initialCommit).put("t2", "v2_2").toBranch(branch));
    assertThrows(ReferenceConflictException.class,
        () -> commit("Conflicting Commit").fromReference(initialCommit).put("t3", "v3_2").toBranch(branch));

    assertThrows(ReferenceConflictException.class,
        () -> commit("Conflicting Commit").fromReference(initialCommit).delete("t1").toBranch(branch));
    assertThrows(ReferenceConflictException.class,
        () -> commit("Conflicting Commit").fromReference(initialCommit).delete("t2").toBranch(branch));
    assertThrows(ReferenceConflictException.class,
        () -> commit("Conflicting Commit").fromReference(initialCommit).delete("t3").toBranch(branch));

    // Checking the state hasn't changed
    assertThat(versionStore().toHash(branch), is(secondCommit));
  }

  /*
   * Test:
   * - Create a new branch
   * - Add a commit to create 2 keys
   * - Add a second commit to delete one key and add a new one
   * - force commit put operations
   * - Check that put operations against 1st commit for the 3 keys fail
   * - Check that delete operations against 1st commit for the 3 keys fail
   * - Check that unchanged operations against 1st commit for the 3 keys fail
   * - Check that branch state hasn't changed
   */
  @Test
  public void forceCommitConflictingOperations() throws Exception {
    final BranchName branch = BranchName.of("foo");

    versionStore().create(branch, Optional.empty());

    commit("Initial Commit")
        .put("t1", "v1_1")
        .put("t2", "v2_1")
        .toBranch(branch);

    commit("Second Commit")
        .put("t1", "v1_2")
        .delete("t2")
        .put("t3", "v3_1")
        .toBranch(branch);

    final Hash putCommit = forceCommit("Conflicting Commit")
        .put("t1", "v1_3")
        .put("t2", "v2_2")
        .put("t3", "v3_2")
        .toBranch(branch);

    assertThat(versionStore().toHash(branch), is(putCommit));
    assertThat(versionStore().getValues(branch, Arrays.asList(Key.of("t1"), Key.of("t2"), Key.of("t3"))),
        contains(
            Optional.of("v1_3"),
            Optional.of("v2_2"),
            Optional.of("v3_2")
        ));

    final Hash unchangedCommit = commit("Conflicting Commit")
        .unchanged("t1")
        .unchanged("t2")
        .unchanged("t3")
        .toBranch(branch);
    assertThat(versionStore().toHash(branch), is(unchangedCommit));
    assertThat(versionStore().getValues(branch, Arrays.asList(Key.of("t1"), Key.of("t2"), Key.of("t3"))),
        contains(
            Optional.of("v1_3"),
            Optional.of("v2_2"),
            Optional.of("v3_2")
        ));

    final Hash deleteCommit = commit("Conflicting Commit")
        .delete("t1")
        .delete("t2")
        .delete("t3")
        .toBranch(branch);
    assertThat(versionStore().toHash(branch), is(deleteCommit));
    assertThat(versionStore().getValues(branch, Arrays.asList(Key.of("t1"), Key.of("t2"), Key.of("t3"))),
        contains(
            Optional.empty(),
            Optional.empty(),
            Optional.empty()
        ));
  }

  /*
   * Test:
   *  - Check that store allows storing the same value under different keys
   */
  @Test
  public void commitDuplicateValues() throws Exception {
    BranchName branch = BranchName.of("dupe-values");
    versionStore().create(branch, Optional.empty());
    versionStore().commit(branch, Optional.empty(), "metadata", ImmutableList.of(
        put("keyA", "foo"),
        put("keyB", "foo"))
    );

    assertThat(versionStore().getValue(branch, Key.of("keyA")), is("foo"));
    assertThat(versionStore().getValue(branch, Key.of("keyB")), is("foo"));
  }

  /*
   * Test:
   * - Check that store throws RNFE if branch doesn't exist
   */
  @Test
  public void commitWithInvalidBranch() {
    final BranchName branch = BranchName.of("unknown");

    assertThrows(ReferenceNotFoundException.class,
        () -> versionStore().commit(branch, Optional.empty(), "New commit", Collections.emptyList()));
  }

  /*
   * Test:
   * - Check that store throws RNFE if reference hash doesn't exist
   */
  @Test
  public void commitWithUnknownReference() throws ReferenceNotFoundException, ReferenceAlreadyExistsException {
    final BranchName branch = BranchName.of("foo");
    versionStore().create(branch, Optional.empty());

    assertThrows(ReferenceNotFoundException.class,
        () -> versionStore().commit(branch, Optional.of(Hash.of("1234567890abcdef")), "New commit", Collections.emptyList()));
  }

  /*
   * Test:
   * - Check that store throws IllegalArgumentException if reference hash is not in branch ancestry
   */
  @Test
  public void commitWithInvalidReference() throws ReferenceNotFoundException, ReferenceConflictException, ReferenceAlreadyExistsException {
    final BranchName branch = BranchName.of("foo");
    versionStore().create(branch, Optional.empty());


    final Hash initialHash = versionStore().toHash(branch);
    versionStore().commit(branch, Optional.of(initialHash), "Some commit", Collections.emptyList());

    final Hash commitHash = versionStore().toHash(branch);

    final BranchName branch2 = BranchName.of("bar");
    versionStore().create(branch2, Optional.empty());

    assertThrows(ReferenceNotFoundException.class,
        () -> versionStore().commit(branch2, Optional.of(commitHash), "Another commit", Collections.emptyList()));
  }

  @Test
  public void getValueForEmptyBranch() throws ReferenceNotFoundException, ReferenceAlreadyExistsException {
    BranchName branch = BranchName.of("empty-branch");
    versionStore().create(branch, Optional.empty());
    final Hash hash = versionStore().toHash(branch);

    assertThat(versionStore().getValue(hash, Key.of("arbitrary")), is(nullValue()));
  }

  @Test
  public void assign() throws VersionStoreException {
    final BranchName branch = BranchName.of("foo");
    versionStore().create(branch, Optional.empty());
    final Hash initialHash = versionStore().toHash(branch);

    final Hash commit = commit("Some commit").toBranch(branch);
    versionStore().create(BranchName.of("bar"), Optional.of(commit));
    versionStore().create(TagName.of("tag1"),  Optional.of(commit));
    versionStore().create(TagName.of("tag2"),  Optional.of(commit));
    versionStore().create(TagName.of("tag3"),  Optional.of(commit));

    final Hash anotherCommit = commit("Another commit").toBranch(branch);
    versionStore().assign(TagName.of("tag2"), Optional.of(commit), anotherCommit);
    versionStore().assign(TagName.of("tag3"), Optional.empty(), anotherCommit);

    assertThrows(ReferenceNotFoundException.class,
        () -> versionStore().assign(BranchName.of("baz"), Optional.empty(), anotherCommit));
    assertThrows(ReferenceNotFoundException.class,
        () -> versionStore().assign(TagName.of("unknowon-tag"), Optional.empty(), anotherCommit));

    assertThrows(ReferenceConflictException.class,
        () -> versionStore().assign(TagName.of("tag1"), Optional.of(initialHash), commit));
    assertThrows(ReferenceConflictException.class,
        () -> versionStore().assign(TagName.of("tag1"), Optional.of(initialHash), anotherCommit));
    assertThrows(ReferenceNotFoundException.class,
        () -> versionStore().assign(TagName.of("tag1"), Optional.of(commit), Hash.of("1234567890abcdef")));

    assertThat(versionStore().getCommits(branch).collect(Collectors.toList()), contains(
        WithHash.of(anotherCommit, "Another commit"),
        WithHash.of(commit, "Some commit")
    ));

    assertThat(versionStore().getCommits(BranchName.of("bar")).collect(Collectors.toList()), contains(
        WithHash.of(commit, "Some commit")
    ));

    assertThat(versionStore().getCommits(TagName.of("tag1")).collect(Collectors.toList()), contains(
        WithHash.of(commit, "Some commit")
    ));

    assertThat(versionStore().getCommits(TagName.of("tag2")).collect(Collectors.toList()), contains(
        WithHash.of(anotherCommit, "Another commit"),
        WithHash.of(commit, "Some commit")
    ));
  }

  @Nested
  @DisplayName("when transplanting")
  protected class WhenTransplanting {
    private Hash initialHash;
    private Hash firstCommit;
    private Hash secondCommit;
    private Hash thirdCommit;

    @BeforeEach
    protected void setupCommits() throws VersionStoreException {
      final BranchName branch = BranchName.of("foo");
      versionStore().create(branch, Optional.empty());

      initialHash = versionStore().toHash(branch);

      firstCommit = commit("Initial Commit")
          .put("t1", "v1_1")
          .put("t2", "v2_1")
          .put("t3", "v3_1")
          .toBranch(branch);

      secondCommit = commit("Second Commit")
          .put("t1", "v1_2")
          .delete("t2")
          .delete("t3")
          .put("t4", "v4_1")
          .toBranch(branch);

      thirdCommit = commit("Third Commit")
          .put("t2", "v2_2")
          .unchanged("t4")
          .toBranch(branch);
    }

    @Test
    protected void checkTransplantOnEmptyBranch() throws VersionStoreException {
      final BranchName newBranch = BranchName.of("bar_1");
      versionStore().create(newBranch, Optional.empty());

      versionStore().transplant(newBranch, Optional.of(initialHash), Arrays.asList(firstCommit, secondCommit, thirdCommit));
      assertThat(versionStore().getValues(newBranch, Arrays.asList(Key.of("t1"), Key.of("t2"), Key.of("t3"), Key.of("t4"))),
          contains(
              Optional.of("v1_2"),
              Optional.of("v2_2"),
              Optional.empty(),
              Optional.of("v4_1")
      ));
    }

    @Test
    protected void checkTransplantWithPreviousCommit() throws VersionStoreException {
      final BranchName newBranch = BranchName.of("bar_2");
      versionStore().create(newBranch, Optional.empty());
      commit("Unrelated commit").put("t5", "v5_1").toBranch(newBranch);

      versionStore().transplant(newBranch, Optional.of(initialHash), Arrays.asList(firstCommit, secondCommit, thirdCommit));
      assertThat(versionStore().getValues(newBranch, Arrays.asList(Key.of("t1"), Key.of("t2"), Key.of("t3"), Key.of("t4"), Key.of("t5"))),
          contains(
              Optional.of("v1_2"),
              Optional.of("v2_2"),
              Optional.empty(),
              Optional.of("v4_1"),
              Optional.of("v5_1")
      ));
    }

    @Test
    protected void checkTransplantWitConflictingCommit() throws VersionStoreException {
      final BranchName newBranch = BranchName.of("bar_3");
      versionStore().create(newBranch, Optional.empty());
      commit("Another commit").put("t1", "v1_4").toBranch(newBranch);

      assertThrows(ReferenceConflictException.class,
          () -> versionStore().transplant(newBranch, Optional.of(initialHash), Arrays.asList(firstCommit, secondCommit, thirdCommit)));
    }

    @Test
    protected void checkTransplantWithDelete() throws VersionStoreException {
      final BranchName newBranch = BranchName.of("bar_4");
      versionStore().create(newBranch, Optional.empty());
      commit("Another commit").put("t1", "v1_4").toBranch(newBranch);
      commit("Another commit").delete("t1").toBranch(newBranch);

      versionStore().transplant(newBranch, Optional.of(initialHash), Arrays.asList(firstCommit, secondCommit, thirdCommit));
      assertThat(versionStore().getValues(newBranch, Arrays.asList(Key.of("t1"), Key.of("t2"), Key.of("t3"), Key.of("t4"))),
          contains(
              Optional.of("v1_2"),
              Optional.of("v2_2"),
              Optional.empty(),
              Optional.of("v4_1")
      ));
    }

    @Test
    protected void checkTransplantOnNonExistingBranch() throws VersionStoreException {
      final BranchName newBranch = BranchName.of("bar_5");
      assertThrows(ReferenceNotFoundException.class,
          () -> versionStore().transplant(newBranch, Optional.of(initialHash), Arrays.asList(firstCommit, secondCommit, thirdCommit)));
    }

    @Test
    protected void checkTransplantWithNonExistingCommit() throws VersionStoreException {
      final BranchName newBranch = BranchName.of("bar_6");
      versionStore().create(newBranch, Optional.empty());
      assertThrows(ReferenceNotFoundException.class,
          () -> versionStore().transplant(newBranch, Optional.of(initialHash), Arrays.asList(Hash.of("1234567890abcdef"))));
    }

    @Test
    protected void checkTransplantWithNoExpectedHash() throws VersionStoreException {
      final BranchName newBranch = BranchName.of("bar_7");
      versionStore().create(newBranch, Optional.empty());
      commit("Another commit").put("t5", "v5_1").toBranch(newBranch);
      commit("Another commit").put("t1", "v1_4").toBranch(newBranch);

      versionStore().transplant(newBranch, Optional.empty(), Arrays.asList(firstCommit, secondCommit, thirdCommit));
      assertThat(versionStore().getValues(newBranch, Arrays.asList(Key.of("t1"), Key.of("t2"), Key.of("t3"), Key.of("t4"), Key.of("t5"))),
          contains(
              Optional.of("v1_2"),
              Optional.of("v2_2"),
              Optional.empty(),
              Optional.of("v4_1"),
              Optional.of("v5_1")
      ));
    }

    @Test
    protected void checkTransplantWithCommitsInWrongOrder() throws VersionStoreException {
      final BranchName newBranch = BranchName.of("bar_8");
      versionStore().create(newBranch, Optional.empty());

      assertThrows(IllegalArgumentException.class,
          () -> versionStore().transplant(newBranch, Optional.empty(), Arrays.asList(secondCommit, firstCommit, thirdCommit)));
    }

    @Test
    protected void checkInvalidBranchHash() throws VersionStoreException {
      final BranchName anotherBranch = BranchName.of("bar");
      versionStore().create(anotherBranch, Optional.empty());
      final Hash unrelatedCommit = commit("Another Commit")
          .put("t1", "v1_1")
          .put("t2", "v2_1")
          .put("t3", "v3_1")
          .toBranch(anotherBranch);

      final BranchName newBranch = BranchName.of("bar_1");
      versionStore().create(newBranch, Optional.empty());

      assertThrows(ReferenceNotFoundException.class,
          () -> versionStore().transplant(newBranch, Optional.of(unrelatedCommit), Arrays.asList(firstCommit, secondCommit, thirdCommit)));
    }

    @Test
    protected void transplantBasic() throws VersionStoreException {
      final BranchName newBranch = BranchName.of("bar_2");
      versionStore().create(newBranch, Optional.empty());
      commit("Unrelated commit").put("t5", "v5_1").toBranch(newBranch);

      versionStore().transplant(newBranch, Optional.of(initialHash), Arrays.asList(firstCommit, secondCommit));
      assertThat(versionStore().getValues(newBranch, Arrays.asList(Key.of("t1"), Key.of("t4"), Key.of("t5"))),
                 contains(
                   Optional.of("v1_2"),
                   Optional.of("v4_1"),
                   Optional.of("v5_1")
                 ));

    }
  }

  @Nested
  protected class WhenMerging {
    private Hash initialHash;
    private Hash firstCommit;
    private Hash secondCommit;
    private Hash thirdCommit;

    @BeforeEach
    protected void setupCommits() throws VersionStoreException {
      final BranchName branch = BranchName.of("foo");
      versionStore().create(branch, Optional.empty());

      initialHash = versionStore().toHash(branch);

      firstCommit = commit("First Commit").put("t1", "v1_1").put("t2", "v2_1").put("t3", "v3_1").toBranch(branch);
      secondCommit = commit("Second Commit").put("t1", "v1_2").delete("t2").delete("t3").put("t4", "v4_1").toBranch(branch);
      thirdCommit = commit("Third Commit").put("t2", "v2_2").unchanged("t4").toBranch(branch);
    }

    @Test
    protected void mergeIntoEmptyBranch() throws VersionStoreException {
      final BranchName newBranch = BranchName.of("bar_1");
      versionStore().create(newBranch, Optional.empty());

      versionStore().merge(thirdCommit, newBranch, Optional.of(initialHash));
      assertThat(versionStore().getValues(newBranch, Arrays.asList(Key.of("t1"), Key.of("t2"), Key.of("t3"), Key.of("t4"))),
                 contains(
                   Optional.of("v1_2"),
                   Optional.of("v2_2"),
                   Optional.empty(),
                   Optional.of("v4_1")
                 ));

      assertThat(versionStore().toHash(newBranch), is(thirdCommit));
    }

    @Test
    protected void mergeIntoNonConflictingBranch() throws VersionStoreException {
      final BranchName newBranch = BranchName.of("bar_2");
      versionStore().create(newBranch, Optional.empty());
      final Hash newCommit = commit("Unrelated commit").put("t5", "v5_1").toBranch(newBranch);

      versionStore().merge(thirdCommit, newBranch, Optional.empty());
      assertThat(versionStore().getValues(newBranch, Arrays.asList(Key.of("t1"), Key.of("t2"), Key.of("t3"), Key.of("t4"), Key.of("t5"))),
                 contains(
                   Optional.of("v1_2"),
                   Optional.of("v2_2"),
                   Optional.empty(),
                   Optional.of("v4_1"),
                   Optional.of("v5_1")
                 ));

      final List<WithHash<String>> commits = versionStore().getCommits(newBranch).collect(Collectors.toList());
      assertThat(commits.size(), is(4));
      assertThat(commits.get(3).getHash(), is(newCommit));
      assertThat(commits.get(2).getValue(), is("First Commit"));
      assertThat(commits.get(1).getValue(), is("Second Commit"));
      assertThat(commits.get(0).getValue(), is("Third Commit"));
    }

    @Test
    protected void nonEmptyFastForwardMerge() throws VersionStoreException {
      final Key key = Key.of("t1");
      final BranchName etl = BranchName.of("etl");
      final BranchName review = BranchName.of("review");
      versionStore().create(etl, Optional.empty());
      versionStore().create(review, Optional.empty());
      versionStore().commit(etl, Optional.empty(), "commit 1", Arrays.<Operation<String>>asList(Put.of(key, "value1")));
      versionStore().merge(versionStore().toHash(etl), review, Optional.empty());
      versionStore().commit(etl, Optional.empty(), "commit 2", Arrays.<Operation<String>>asList(Put.of(key, "value2")));
      versionStore().merge(versionStore().toHash(etl), review, Optional.empty());
      assertEquals(versionStore().getValue(review, key), "value2");
    }

    @Test
    protected void mergeWithCommonAncestor() throws VersionStoreException {
      final BranchName newBranch = BranchName.of("bar_2");
      versionStore().create(newBranch, Optional.of(firstCommit));

      final Hash newCommit = commit("Unrelated commit").put("t5", "v5_1").toBranch(newBranch);

      versionStore().merge(thirdCommit, newBranch, Optional.empty());
      assertThat(versionStore().getValues(newBranch, Arrays.asList(Key.of("t1"), Key.of("t2"), Key.of("t3"), Key.of("t4"), Key.of("t5"))),
                 contains(
                   Optional.of("v1_2"),
                   Optional.of("v2_2"),
                   Optional.empty(),
                   Optional.of("v4_1"),
                   Optional.of("v5_1")
                 ));

      final List<WithHash<String>> commits = versionStore().getCommits(newBranch).collect(Collectors.toList());
      assertThat(commits.size(), is(4));
      assertThat(commits.get(3).getHash(), is(firstCommit));
      assertThat(commits.get(2).getHash(), is(newCommit));
      assertThat(commits.get(1).getValue(), is("Second Commit"));
      assertThat(commits.get(0).getValue(), is("Third Commit"));
    }

    @Test
    protected void mergeIntoConflictingBranch() throws VersionStoreException {
      final BranchName newBranch = BranchName.of("bar_3");
      versionStore().create(newBranch, Optional.empty());
      commit("Another commit").put("t1", "v1_4").toBranch(newBranch);

      assertThrows(ReferenceConflictException.class,
          () -> versionStore().merge(thirdCommit, newBranch, Optional.of(initialHash)));
    }

    @Test
    protected void mergeIntoNonExistingBranch() throws VersionStoreException {
      final BranchName newBranch = BranchName.of("bar_5");
      assertThrows(ReferenceNotFoundException.class,
          () -> versionStore().merge(thirdCommit, newBranch, Optional.of(initialHash)));
    }

    @Test
    protected void mergeIntoNonExistingReference() throws VersionStoreException {
      final BranchName newBranch = BranchName.of("bar_6");
      versionStore().create(newBranch, Optional.empty());
      assertThrows(ReferenceNotFoundException.class,
          () -> versionStore().merge(Hash.of("1234567890abcdef"), newBranch, Optional.of(initialHash)));
    }
  }

  @Test
  void toRef() throws VersionStoreException {
    final BranchName branch = BranchName.of("main");
    versionStore().create(branch, Optional.empty());
    versionStore().toHash(branch);

    final Hash firstCommit = commit("First Commit").toBranch(branch);
    final Hash secondCommit = commit("Second Commit").toBranch(branch);
    final Hash thirdCommit = commit("Third Commit").toBranch(branch);

    versionStore().create(BranchName.of(thirdCommit.asString()), Optional.of(firstCommit));
    versionStore().create(TagName.of(secondCommit.asString()), Optional.of(firstCommit));

    assertThat(versionStore().toRef(firstCommit.asString()), is(WithHash.of(firstCommit, firstCommit)));
    assertThat(versionStore().toRef(secondCommit.asString()), is(WithHash.of(firstCommit, TagName.of(secondCommit.asString()))));
    assertThat(versionStore().toRef(thirdCommit.asString()), is(WithHash.of(firstCommit, BranchName.of(thirdCommit.asString()))));
    // Is it correct to allow a reference with the sentinel reference?
    //assertThat(store().toRef(initialCommit.asString()), is(WithHash.of(initialCommit, initialCommit)));
    assertThrows(ReferenceNotFoundException.class, () -> versionStore().toRef("unknown-ref"));
    assertThrows(ReferenceNotFoundException.class, () -> versionStore().toRef("1234567890abcdef"));
  }

  @Test
  protected void checkDiff() throws VersionStoreException {
    final BranchName branch = BranchName.of("main");
    versionStore().create(branch, Optional.empty());
    final Hash initial = versionStore().toHash(branch);

    final Hash firstCommit = commit("First Commit").put("k1", "v1").put("k2", "v2").toBranch(branch);
    final Hash secondCommit = commit("Second Commit").put("k2", "v2a").put("k3", "v3").toBranch(branch);

    List<Diff<String>> startToSecond = versionStore().getDiffs(initial, secondCommit).collect(Collectors.toList());
    assertThat(startToSecond, containsInAnyOrder(
        Diff.of(Key.of("k1"), Optional.empty(), Optional.of("v1")),
        Diff.of(Key.of("k2"), Optional.empty(), Optional.of("v2a")),
        Diff.of(Key.of("k3"), Optional.empty(), Optional.of("v3"))
    ));

    List<Diff<String>> secondToStart = versionStore().getDiffs(secondCommit, initial).collect(Collectors.toList());
    assertThat(secondToStart, containsInAnyOrder(
        Diff.of(Key.of("k1"), Optional.of("v1"), Optional.empty()),
        Diff.of(Key.of("k2"), Optional.of("v2a"), Optional.empty()),
        Diff.of(Key.of("k3"), Optional.of("v3"), Optional.empty())
    ));

    List<Diff<String>> firstToSecond = versionStore().getDiffs(firstCommit, secondCommit).collect(Collectors.toList());
    assertThat(firstToSecond, containsInAnyOrder(
        Diff.of(Key.of("k2"), Optional.of("v2"), Optional.of("v2a")),
        Diff.of(Key.of("k3"), Optional.empty(), Optional.of("v3"))
    ));

    List<Diff<String>> firstToFirst = versionStore().getDiffs(firstCommit, firstCommit).collect(Collectors.toList());
    assertTrue(firstToFirst.isEmpty());
  }

  @Test
  void checkDuplicateValueCommit() throws Exception {
    BranchName branch = BranchName.of("dupe-values");
    versionStore().create(branch, Optional.empty());
    versionStore().commit(branch, Optional.empty(), "metadata", ImmutableList.of(
        Put.of(Key.of("hi"), "world"),
        Put.of(Key.of("no"), "world"))
    );

    assertEquals("world", versionStore().getValue(branch, Key.of("hi")));
    assertEquals("world", versionStore().getValue(branch, Key.of("no")));
  }

  @Test
  void mergeToEmpty() throws Exception {
    BranchName branch1 = BranchName.of("b1");
    BranchName branch2 = BranchName.of("b2");
    versionStore().create(branch1, Optional.empty());
    versionStore().create(branch2, Optional.empty());
    versionStore().commit(branch2, Optional.empty(), "metadata", ImmutableList.of(
        Put.of(Key.of("hi"), "world"),
        Put.of(Key.of("no"), "world")));
    versionStore().merge(versionStore().toHash(branch2), branch1, Optional.of(versionStore().toHash(branch1)));
  }

  @Test
  void mergeNoConflict() throws Exception {
    BranchName branch1 = BranchName.of("b1");
    BranchName branch2 = BranchName.of("b2");
    Hash initial1 = versionStore().create(branch1, Optional.empty());
    Hash commit1 = versionStore().commit(branch1, Optional.empty(), "metadata", ImmutableList.of(
        Put.of(Key.of("foo"), "world1"),
        Put.of(Key.of("bar"), "world2")));
    assertNotEquals(initial1, commit1);

    Hash initial2 = versionStore().create(branch2, Optional.empty());
    Hash commit2 = versionStore().commit(branch2, Optional.empty(), "metadata", ImmutableList.of(
        Put.of(Key.of("hi"), "world3"),
        Put.of(Key.of("no"), "world4")));
    assertNotEquals(initial2, commit2);
    versionStore().merge(versionStore().toHash(branch2), branch1, Optional.of(versionStore().toHash(branch1)));

    assertEquals("world1", versionStore().getValue(branch1, Key.of("foo")));
    assertEquals("world2", versionStore().getValue(branch1, Key.of("bar")));
    assertEquals("world3", versionStore().getValue(branch1, Key.of("hi")));
    assertEquals("world4", versionStore().getValue(branch1, Key.of("no")));
  }

  @Test
  protected void mergeConflict() throws Exception {
    BranchName branch1 = BranchName.of("b1");
    BranchName branch2 = BranchName.of("b2");
    Hash initial1 = versionStore().create(branch1, Optional.empty());
    Hash commit1 = versionStore().commit(branch1, Optional.empty(), "metadata",
        ImmutableList.of(Put.of(Key.of("conflictKey"), "world1")));
    assertNotEquals(initial1, commit1);

    Hash initial2 = versionStore().create(branch2, Optional.empty());
    Hash commit2 = versionStore().commit(branch2, Optional.empty(), "metadata2",
        ImmutableList.of(Put.of(Key.of("conflictKey"), "world2")));
    assertNotEquals(initial2, commit2);

    ReferenceConflictException ex = assertThrows(ReferenceConflictException.class, () ->
        versionStore().merge(versionStore().toHash(branch2), branch1, Optional.of(versionStore().toHash(branch1))));
    assertThat(ex.getMessage(), Matchers.containsString("conflictKey"));
  }

  @Test
  void checkKeyList() throws Exception {
    BranchName branch = BranchName.of("my-key-list");
    versionStore().create(branch, Optional.empty());
    versionStore().commit(branch, Optional.empty(), "metadata", ImmutableList.of(
        Put.of(Key.of("hi"), "world"),
        Put.of(Key.of("no"), "world"),
        Put.of(Key.of("mad mad"), "world")));
    assertThat(versionStore().getKeys(branch).map(WithType::getValue).map(Key::toString).collect(ImmutableSet.toImmutableSet()),
        Matchers.containsInAnyOrder("hi", "no", "mad mad"));
  }

  @Test
  void ensureKeyCheckpointsAndMultiFragmentsWork() throws Exception {
    BranchName branch = BranchName.of("lots-of-keys");
    Hash initial = versionStore().create(branch, Optional.empty());
    Hash current = versionStore().toHash(branch);
    assertEquals(current, initial);
    Random r = new Random(1234);
    char[] longName = new char[4096];
    Arrays.fill(longName, 'a');
    String prefix = new String(longName);
    List<Key> names = new LinkedList<>();
    Hash last = current;
    for (int i = 1; i < 200; i++) {
      Hash commitHash;
      if (i % 5 == 0) {
        // every so often, remove a key.
        Key removal = names.remove(r.nextInt(names.size()));
        commitHash = versionStore().commit(branch, Optional.of(current), "commit " + i, Collections.singletonList(Delete.of(removal)));
      } else {
        Key name = Key.of(prefix + i);
        names.add(name);
        commitHash = versionStore().commit(branch, Optional.of(current), "commit " + i, Collections.singletonList(Put.of(name, "bar")));
      }
      current = versionStore().toHash(branch);
      assertEquals(current, commitHash);
      assertNotEquals(last, current);
      last = current;
    }

    List<Key> keysFromStore = versionStore().getKeys(branch).map(WithType::getValue).collect(Collectors.toList());

    // ensure that our total key size is greater than a single dynamo page.
    assertThat(keysFromStore.size() * longName.length, Matchers.greaterThan(400000));

    // ensure that keys stored match those expected.
    assertThat(keysFromStore, Matchers.containsInAnyOrder(names.toArray(new Key[0])));
  }

  @Test
  void multiload() throws Exception {
    BranchName branch = BranchName.of("my-key-list");
    Hash initialHash = versionStore().create(branch, Optional.empty());
    Hash commitHash = versionStore().commit(branch, Optional.empty(), "metadata", ImmutableList.of(
        Put.of(Key.of("hi"), "world1"),
        Put.of(Key.of("no"), "world2"),
        Put.of(Key.of("mad mad"), "world3")));
    assertNotEquals(initialHash, commitHash);

    assertEquals(
        Arrays.asList("world1", "world2", "world3"),
        versionStore().getValues(branch, Arrays.asList(Key.of("hi"), Key.of("no"), Key.of("mad mad")))
          .stream()
          .map(Optional::get)
          .collect(Collectors.toList()));
  }

  @Test
  void ensureValidEmptyBranchState() throws ReferenceNotFoundException, ReferenceAlreadyExistsException {
    BranchName branch = BranchName.of("empty_branch");
    versionStore().create(branch, Optional.empty());
    Hash hash = versionStore().toHash(branch);
    assertNull(versionStore().getValue(hash, Key.of("arbitrary")));
  }

  @Test
  void conflictingCommit() throws Exception {
    BranchName branch = BranchName.of("conflictingCommit");
    Hash createHash = versionStore().create(branch, Optional.empty());

    // first commit.
    Hash initialHash = versionStore().commit(branch, Optional.empty(), "metadata",
        ImmutableList.of(Put.of(Key.of("hi"), "hello world")));
    assertNotEquals(createHash, initialHash);

    //first hash.
    Hash originalHash = versionStore().getCommits(branch).findFirst().get().getHash();
    assertEquals(initialHash, originalHash);

    //second commit.
    Hash firstHash = versionStore().commit(branch, Optional.empty(), "metadata",
        ImmutableList.of(Put.of(Key.of("hi"), "goodbye world")));
    assertNotEquals(originalHash, firstHash);

    // do an extra commit to make sure it has a different hash even though it has the same value.
    Hash secondHash = versionStore().commit(branch, Optional.empty(), "metadata",
        ImmutableList.of(Put.of(Key.of("hi"), "goodbye world")));
    assertNotEquals(originalHash, secondHash);
    assertNotEquals(firstHash, secondHash);

    //attempt commit using first hash which has conflicting key change.
    assertThrows(ReferenceConflictException.class, () -> versionStore().commit(branch, Optional.of(originalHash),
        "metadata", ImmutableList.of(Put.of(Key.of("hi"), "my world"))));
  }

  @Test
  void conflictingCommitWithHash() throws Exception {
    BranchName branch = BranchName.of("conflictingCommitWithHash");
    Hash createHash = versionStore().create(branch, Optional.empty());

    // first commit.
    Hash initialHash = versionStore().commit(branch, Optional.of(createHash), "metadata",
        ImmutableList.of(Put.of(Key.of("hi"), "hello world")));
    assertNotEquals(createHash, initialHash);

    //first hash.
    Hash originalHash = versionStore().getCommits(branch).findFirst().get().getHash();
    assertEquals(initialHash, originalHash);

    //second commit.
    Hash firstHash = versionStore().commit(branch, Optional.of(originalHash), "metadata",
        ImmutableList.of(Put.of(Key.of("hi"), "goodbye world")));
    assertNotEquals(originalHash, firstHash);

    // do an extra commit to make sure it has a different hash even though it has the same value.
    Hash secondHash = versionStore().commit(branch, Optional.of(firstHash), "metadata",
        ImmutableList.of(Put.of(Key.of("hi"), "goodbye world")));
    assertNotEquals(originalHash, secondHash);
    assertNotEquals(firstHash, secondHash);

    //attempt commit using first hash which has conflicting key change.
    assertThrows(ReferenceConflictException.class, () -> versionStore().commit(branch, Optional.of(originalHash),
        "metadata", ImmutableList.of(Put.of(Key.of("hi"), "my world"))));
  }

  @Test
  void assignments() throws Exception {
    BranchName branch = BranchName.of("foo");
    final Key k1 = Key.of("p1");
    versionStore().create(branch, Optional.empty());
    versionStore().commit(branch, Optional.empty(), "c1", ImmutableList.of(Put.of(k1, "v1")));
    Hash c1 = versionStore().toHash(branch);
    versionStore().commit(branch, Optional.empty(), "c1", ImmutableList.of(Put.of(k1, "v2")));
    Hash c2 = versionStore().toHash(branch);
    TagName t1 = TagName.of("t1");
    BranchName b2 = BranchName.of("b2");

    // ensure tag create assignment is correct.
    versionStore().create(t1, Optional.of(c1));
    assertEquals("v1", versionStore().getValue(t1, k1));

    // ensure branch create non-assignment works
    versionStore().create(b2, Optional.empty());
    assertEquals(null, versionStore().getValue(b2, k1));

    // ensure tag reassignment is correct.
    versionStore().assign(t1, Optional.of(c1), c2);
    assertEquals("v2", versionStore().getValue(t1, k1));

    // ensure branch assignment (no current) is correct
    versionStore().assign(b2, Optional.empty(), c1);
    assertEquals("v1", versionStore().getValue(b2, k1));

    // ensure branch assignment (with current) is current
    versionStore().assign(b2, Optional.of(c1), c2);
    assertEquals("v2", versionStore().getValue(b2, k1));

  }

  @Test
  void delete() throws Exception {
    BranchName branch = BranchName.of("foo");
    final Key k1 = Key.of("p1");
    versionStore().create(branch, Optional.empty());
    versionStore().commit(branch, Optional.empty(), "c1", ImmutableList.of(Put.of(k1, "v1")));
    assertEquals("v1", versionStore().getValue(branch, k1));

    versionStore().commit(branch, Optional.empty(), "c1", ImmutableList.of(Delete.of(k1)));
    assertEquals(null, versionStore().getValue(branch, k1));
  }

  @Test
  void unchangedOperation() throws Exception {
    BranchName branch = BranchName.of("foo");
    versionStore().create(branch, Optional.empty());
    // first commit.
    versionStore().commit(branch, Optional.empty(), "metadata", ImmutableList.of(Put.of(Key.of("hi"), "hello world")));

    //first hash.
    Hash originalHash = versionStore().getCommits(branch).findFirst().get().getHash();

    //second commit.
    versionStore().commit(branch, Optional.empty(), "metadata", ImmutableList.of(Put.of(Key.of("hi"), "goodbye world")));

    versionStore().commit(branch, Optional.empty(), "metadata", ImmutableList.of(Put.of(Key.of("hi"), "goodbye world")));

    //attempt commit using first hash which has conflicting key change.
    assertThrows(ReferenceConflictException.class, () -> versionStore().commit(branch, Optional.of(originalHash),
        "metadata", ImmutableList.of(Put.of(Key.of("hi"), "my world"))));

    // attempt commit using first hash, put on on-conflicting key, unchanged on conflicting key.
    assertThrows(ReferenceConflictException.class,
        () -> versionStore().commit(branch, Optional.of(originalHash), "metadata",
            ImmutableList.of(Put.of(Key.of("bar"), "mellow"), Unchanged.of(Key.of("hi")))));
  }

  @Test
  void checkEmptyHistory() throws Exception {
    BranchName branch = BranchName.of("foo");
    versionStore().create(branch, Optional.empty());
    assertEquals(0L, versionStore().getCommits(branch).count());
  }

  @Test
  void completeFlow() throws Exception {
    final BranchName branch = ImmutableBranchName.builder().name("main").build();
    final BranchName branch2 = ImmutableBranchName.builder().name("b2").build();
    final TagName tag = ImmutableTagName.builder().name("t1").build();
    final Key p1 = ImmutableKey.builder().addElements("my.path").build();
    final String commit1 = "my commit 1";
    final String commit2 = "my commit 2";
    final String v1 = "my.value";
    final String v2 = "my.value2";

    // create a branch
    versionStore().create(branch, Optional.empty());

    assertThrows(ReferenceAlreadyExistsException.class, () -> versionStore().create(branch, Optional.empty()));

    versionStore().commit(branch, Optional.empty(), commit1, ImmutableList.of(
        ImmutablePut.<String>builder().key(p1)
            .shouldMatchHash(false)
            .value(v1)
            .build()
        )
    );

    assertEquals(v1, versionStore().getValue(branch, p1));

    versionStore().create(tag, Optional.of(versionStore().toHash(branch)));

    versionStore().commit(branch, Optional.empty(), commit2, ImmutableList.of(
        ImmutablePut.<String>builder().key(
            p1
            )
        .shouldMatchHash(false)
        .value(v2)
        .build()
        )
    );

    assertEquals(v2, versionStore().getValue(branch, p1));
    assertEquals(v1, versionStore().getValue(tag, p1));

    List<WithHash<String>> commits = versionStore().getCommits(branch).collect(Collectors.toList());

    assertEquals(v1, versionStore().getValue(commits.get(1).getHash(), p1));
    assertEquals(commit1, commits.get(1).getValue());
    assertEquals(v2, versionStore().getValue(commits.get(0).getHash(), p1));
    assertEquals(commit2, commits.get(0).getValue());

    versionStore().assign(tag, Optional.of(commits.get(1).getHash()), commits.get(0).getHash());

    assertEquals(commits, versionStore().getCommits(tag).collect(Collectors.toList()));
    assertEquals(commits, versionStore().getCommits(commits.get(0).getHash()).collect(Collectors.toList()));

    try (Stream<WithHash<NamedRef>> str = versionStore().getNamedRefs()) {
      assertEquals(2, str.count());
    }

    versionStore().create(branch2, Optional.of(commits.get(1).getHash()));

    versionStore().delete(branch, Optional.of(commits.get(0).getHash()));

    try (Stream<WithHash<NamedRef>> str = versionStore().getNamedRefs()) {
      assertEquals(2, str.count());
    }

    assertEquals(v1, versionStore().getValue(branch2, p1));
  }

  @Test
  void unknownRef() throws Exception {
    BranchName branch = BranchName.of("bar");
    versionStore().create(branch, Optional.empty());
    versionStore().commit(branch, Optional.empty(), "metadata", ImmutableList.of(Put.of(Key.of("hi"), "hello world")));
    TagName tag = TagName.of("foo");
    Hash expected = versionStore().toHash(branch);
    versionStore().create(tag, Optional.of(expected));

    testRefMatchesToRef(branch, expected, branch.getName());
    testRefMatchesToRef(tag, expected, tag.getName());
    testRefMatchesToRef(expected, expected, expected.asString());
  }

  private void testRefMatchesToRef(Ref ref, Hash hash, String name) throws ReferenceNotFoundException {
    WithHash<Ref> val = versionStore().toRef(name);
    assertEquals(ref, val.getValue());
    assertEquals(hash, val.getHash());
  }

  @Test
  void checkValueEntityType() throws Exception {
    BranchName branch = BranchName.of("entity-types");
    versionStore().create(branch, Optional.empty());

    // have to do this here as tiered store stores payload at commit time
    Mockito.doReturn(StringSerializer.TestEnum.NO).when(StringSerializer.getInstance()).getType("world");
    versionStore().commit(branch, Optional.empty(), "metadata", ImmutableList.of(
        Put.of(Key.of("hi"), "world"))
    );

    assertEquals("world", versionStore().getValue(branch, Key.of("hi")));
    List<Optional<String>> values = versionStore().getValues(branch, Lists.newArrayList(Key.of("hi")));
    assertEquals(1, values.size());
    assertTrue(values.get(0).isPresent());

    // have to do this here as non-tiered store reads payload when getKeys is called
    Mockito.doReturn(StringSerializer.TestEnum.NO).when(StringSerializer.getInstance()).getType("world");
    List<WithType<Key, StringSerializer.TestEnum>> keys = versionStore().getKeys(branch).collect(Collectors.toList());

    assertEquals(1, keys.size());
    assertEquals(Key.of("hi"), keys.get(0).getValue());
    assertEquals(StringSerializer.TestEnum.NO, keys.get(0).getType());
  }


  protected CommitBuilder<String, String, StringSerializer.TestEnum> forceCommit(String message) {
    return new CommitBuilder<>(versionStore()).withMetadata(message);
  }

  protected CommitBuilder<String, String, StringSerializer.TestEnum> commit(String message) {
    return new CommitBuilder<>(versionStore()).withMetadata(message).fromLatest();
  }

  protected Put<String> put(String key, String value) {
    return Put.of(Key.of(key), value);
  }
}
