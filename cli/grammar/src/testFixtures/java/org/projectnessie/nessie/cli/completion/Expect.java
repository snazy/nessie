/*
 * Copyright (C) 2024 Dremio
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
package org.projectnessie.nessie.cli.completion;

import java.util.List;
import java.util.Objects;

public class Expect {
  final String name;
  final List<List<String>> rules;
  final boolean rule;

  Expect(String name, List<List<String>> rules, boolean rule) {
    this.name = name;
    this.rules = rules;
    this.rule = rule;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;

    Expect expect = (Expect) o;
    return Objects.equals(name, expect.name)
        && Objects.equals(rules, expect.rules)
        && rule == expect.rule;
  }

  @Override
  public int hashCode() {
    int result = Objects.hashCode(name);
    result = 31 * result + (rule ? 1 : 0);
    result = 31 * result + Objects.hashCode(rules);
    return result;
  }

  @Override
  public String toString() {
    return "Expect{" + (rule ? "rule '" : "label '") + name + "' rules=" + rules + '}';
  }
}
