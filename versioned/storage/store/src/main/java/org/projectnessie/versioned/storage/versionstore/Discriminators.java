/*
 * Copyright (C) 2023 Dremio
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

public enum Discriminators {
  CONTENT_DISCRIMINATOR("C"),
  DOCUMENTATION_DISCRIMINATOR("D");

  private final String value;

  Discriminators(String value) {
    this.value = value;
  }

  public static Discriminators fromString(String value) {
    for (Discriminators disc : Discriminators.values()) {
      if (disc.value().equals(value)) {
        return disc;
      }
    }
    return null;
  }

  public String value() {
    return value;
  }
}
