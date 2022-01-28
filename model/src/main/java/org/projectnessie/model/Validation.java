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
package org.projectnessie.model;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/** Collection of validation rules. ADD SOME DOCS. */
public final class Validation {
  public static final String HASH_REGEX = "^[0-9a-fA-F]{8,64}$";
  public static final String REF_NAME_REGEX =
      "^[A-Za-z](((?![.][.])[A-Za-z0-9./_-])*[A-Za-z0-9._-])?$";
  public static final String REF_NAME_OR_HASH_REGEX =
      "^(([0-9a-fA-F]{8,64})|([A-Za-z](((?![.][.])[A-Za-z0-9./_-])*[A-Za-z0-9._-])?))$";

  public static final Pattern HASH_PATTERN = Pattern.compile(HASH_REGEX);
  public static final Pattern REF_NAME_PATTERN = Pattern.compile(REF_NAME_REGEX);
  public static final Pattern REF_NAME_OR_HASH_PATTERN = Pattern.compile(REF_NAME_OR_HASH_REGEX);

  private static final String HASH_RULE = "consist of the hex representation of 4-32 bytes";
  private static final String REF_RULE =
      "start with a letter, followed by letters, digits, a ./_- character, "
          + "not end with a slash, not contain ..";

  public static final String HASH_MESSAGE = "Hash must " + HASH_RULE;
  public static final String REF_NAME_MESSAGE = "Reference name must " + REF_RULE;
  public static final String REF_NAME_OR_HASH_MESSAGE =
      "Reference must be either a reference name or hash, " + REF_RULE + " or " + HASH_RULE;
  public static final String FORBIDDEN_REF_NAME_MESSAGE =
      "Reference name mut not be HEAD, BARE or a potential commit ID representation.";
  public static final Set<String> FORBIDDEN_REF_NAMES =
      Collections.unmodifiableSet(new HashSet<>(Arrays.asList("HEAD", "BARE")));

  private Validation() {
    // empty
  }

  /**
   * Just checks whether a string is a valid reference-name, but doesn't throw an exception.
   *
   * @see #validateReferenceName(String)
   */
  public static boolean isValidReferenceName(String referenceName) {
    Objects.requireNonNull(referenceName, "referenceName must not be null");
    Matcher matcher = REF_NAME_PATTERN.matcher(referenceName);
    return matcher.matches();
  }

  /**
   * Just checks whether a string is a valid hash, but doesn't throw an exception.
   *
   * @see #validateHash(String)
   */
  public static boolean isValidHash(String hash) {
    Objects.requireNonNull(hash, "hash must not be null");
    Matcher matcher = HASH_PATTERN.matcher(hash);
    return matcher.matches();
  }

  /**
   * Just checks whether a string is a valid reference-name (as per {@link
   * #isValidReferenceName(String)}) or a valid hash (as per {@link #isValidHash(String)}).
   */
  public static boolean isValidReferenceNameOrHash(String ref) {
    Objects.requireNonNull(ref, "reference (name or hash) must not be null");
    Matcher matcher = REF_NAME_OR_HASH_PATTERN.matcher(ref);
    return matcher.matches();
  }

  /**
   * Validates whether a string is a valid reference-name.
   *
   * <p>The rules are: <em>{@value #REF_RULE}</em>
   *
   * @param referenceName the reference name string to test.
   */
  public static String validateReferenceName(String referenceName) throws IllegalArgumentException {
    if (isValidReferenceName(referenceName)) {
      return referenceName;
    }
    throw new IllegalArgumentException(REF_NAME_MESSAGE + " - but was: " + referenceName);
  }

  /**
   * Validates whether a string is a valid hash.
   *
   * <p>The rules are: <em>{@value #HASH_RULE}</em>
   *
   * @param referenceName the reference name string to test.
   */
  public static String validateHash(String referenceName) throws IllegalArgumentException {
    if (isValidHash(referenceName)) {
      return referenceName;
    }
    throw new IllegalArgumentException(HASH_MESSAGE + " - but was: " + referenceName);
  }

  /**
   * Validates whether a string is a valid reference name or hash.
   *
   * <p>See {@link #validateReferenceName(String)} and {@link #validateHash(String)} for the rules.
   *
   * @param ref the reference name string to test.
   */
  public static String validateReferenceNameOrHash(String ref) throws IllegalArgumentException {
    if (isValidReferenceNameOrHash(ref)) {
      return ref;
    }
    throw new IllegalArgumentException(REF_NAME_OR_HASH_MESSAGE + " - but was: " + ref);
  }

  /**
   * Checks whether {@code ref} represents a forbidden reference name ({@code HEAD} or {@code BARE})
   * or could represent a commit-ID.
   *
   * @param ref reference name to check
   * @return {@code true}, if forbidden
   */
  public static boolean isForbiddenReferenceName(String ref) {
    return FORBIDDEN_REF_NAMES.contains(ref) || HASH_PATTERN.matcher(ref).matches();
  }

  /**
   * Throws an {@link IllegalArgumentException} if {@code ref} represents a forbidden reference
   * name, see {@link #isForbiddenReferenceName(String)}.
   *
   * @param ref reference name to check
   * @return {@code ref}
   */
  public static String validateForbiddenReferenceName(String ref) throws IllegalArgumentException {
    if (isForbiddenReferenceName(ref)) {
      throw new IllegalArgumentException(FORBIDDEN_REF_NAME_MESSAGE + " - but was " + ref);
    }
    return ref;
  }
}
