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
package org.projectnessie.api.v1.params;

import java.util.Objects;
import javax.annotation.Nullable;
import javax.validation.constraints.NotNull;
import javax.validation.constraints.Pattern;
import javax.ws.rs.PathParam;
import org.eclipse.microprofile.openapi.annotations.media.ExampleObject;
import org.eclipse.microprofile.openapi.annotations.parameters.Parameter;
import org.projectnessie.model.Validation;

public class DiffParams {

  public static final String HASH_OPTIONAL_REGEX = "(" + Validation.HASH_REGEX + ")?";

  private static final char HASH_SEPARATOR = '*';

  @NotNull
  @jakarta.validation.constraints.NotNull
  @Pattern(regexp = Validation.REF_NAME_REGEX, message = Validation.REF_NAME_MESSAGE)
  @jakarta.validation.constraints.Pattern(
      regexp = Validation.REF_NAME_REGEX,
      message = Validation.REF_NAME_MESSAGE)
  private String fromRef;

  @Nullable
  @jakarta.annotation.Nullable
  @Pattern(regexp = HASH_OPTIONAL_REGEX, message = Validation.HASH_MESSAGE)
  @jakarta.validation.constraints.Pattern(
      regexp = HASH_OPTIONAL_REGEX,
      message = Validation.HASH_MESSAGE)
  private String fromHashOnRef;

  @NotNull
  @jakarta.validation.constraints.NotNull
  @Pattern(regexp = Validation.REF_NAME_REGEX, message = Validation.REF_NAME_MESSAGE)
  @jakarta.validation.constraints.Pattern(
      regexp = Validation.REF_NAME_REGEX,
      message = Validation.REF_NAME_MESSAGE)
  private String toRef;

  @Nullable
  @jakarta.annotation.Nullable
  @Pattern(regexp = HASH_OPTIONAL_REGEX, message = Validation.HASH_MESSAGE)
  @jakarta.validation.constraints.Pattern(
      regexp = HASH_OPTIONAL_REGEX,
      message = Validation.HASH_MESSAGE)
  private String toHashOnRef;

  public DiffParams() {}

  @org.immutables.builder.Builder.Constructor
  DiffParams(
      @NotNull @jakarta.validation.constraints.NotNull String fromRef,
      @Nullable @jakarta.annotation.Nullable String fromHashOnRef,
      @NotNull @jakarta.validation.constraints.NotNull String toRef,
      @Nullable @jakarta.annotation.Nullable String toHashOnRef) {
    this.fromRef = fromRef;
    this.fromHashOnRef = fromHashOnRef;
    this.toRef = toRef;
    this.toHashOnRef = toHashOnRef;
  }

  @Parameter(
      description = "The 'from' reference (and optional hash) to start the diff from",
      examples = {@ExampleObject(ref = "ref"), @ExampleObject(ref = "refForDiffWithHash")})
  @PathParam("fromRefWithHash")
  @jakarta.ws.rs.PathParam("fromRefWithHash")
  public void setFromRefWithHash(String value) {
    this.fromRef = parseRefName(value);
    this.fromHashOnRef = parseHash(value);
  }

  @Parameter(
      description = "The 'to' reference (and optional hash) to end the diff at.",
      examples = {@ExampleObject(ref = "ref"), @ExampleObject(ref = "refForDiffWithHash")})
  @PathParam("toRefWithHash")
  @jakarta.ws.rs.PathParam("toRefWithHash")
  public void setToRefWithHash(String value) {
    this.toRef = parseRefName(value);
    this.toHashOnRef = parseHash(value);
  }

  private String parseRefName(String param) {
    int idx = param.indexOf(HASH_SEPARATOR);
    return idx == 0 ? null : idx < 0 ? param : param.substring(0, idx);
  }

  private String parseHash(String param) {
    int idx = param.indexOf(HASH_SEPARATOR);
    return idx < 0 ? null : param.substring(idx + 1);
  }

  public String getFromRef() {
    return fromRef;
  }

  @Nullable
  @jakarta.annotation.Nullable
  public String getFromHashOnRef() {
    return emptyToNull(fromHashOnRef);
  }

  public String getToRef() {
    return toRef;
  }

  @Nullable
  @jakarta.annotation.Nullable
  public String getToHashOnRef() {
    return emptyToNull(toHashOnRef);
  }

  public String getFromPathParam() {
    return asPathParam(fromRef, fromHashOnRef);
  }

  public String getToPathParam() {
    return asPathParam(toRef, toHashOnRef);
  }

  private String asPathParam(String ref, String hash) {
    if (ref == null) {
      if (hash == null) {
        return null;
      } else {
        return HASH_SEPARATOR + hash;
      }
    } else {
      if (hash == null) {
        return ref;
      } else {
        return ref + HASH_SEPARATOR + hash;
      }
    }
  }

  private static String emptyToNull(String s) {
    if (s == null || s.isEmpty()) {
      return null;
    }
    if (s.charAt(0) == '*') {
      if (s.length() == 1) {
        return null;
      }
      return s.substring(1);
    }
    return s;
  }

  public static DiffParamsBuilder builder() {
    return new DiffParamsBuilder();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof DiffParams)) {
      return false;
    }
    DiffParams that = (DiffParams) o;
    return Objects.equals(fromRef, that.fromRef)
        && Objects.equals(fromHashOnRef, that.fromHashOnRef)
        && Objects.equals(toRef, that.toRef)
        && Objects.equals(toHashOnRef, that.toHashOnRef);
  }

  @Override
  public int hashCode() {
    return Objects.hash(fromRef, fromHashOnRef, toRef, toHashOnRef);
  }
}
