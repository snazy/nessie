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

import java.util.ArrayList;
import java.util.List;
import org.agrona.collections.IntArrayList;

class Suggestion {
  final int id;
  final List<IntArrayList> ctx;
  final boolean isRule;

  Suggestion(int id, IntArrayList ctx, boolean isRule) {
    this.id = id;
    this.ctx = new ArrayList<>();
    this.ctx.add(ctx);
    this.isRule = isRule;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;

    Suggestion that = (Suggestion) o;
    return id == that.id && isRule == that.isRule && ctx.equals(that.ctx);
  }

  @Override
  public int hashCode() {
    int result = id;
    result = 31 * result + (isRule ? 1 : 0);
    result = 31 * result + ctx.hashCode();
    return result;
  }

  void addCtx(Suggestion suggestion) {
    ctx.addAll(suggestion.ctx);
  }

  @Override
  public String toString() {
    return "Suggestion{" + (isRule ? "rule #" : "label #") + id + ", ctx=" + ctx + '}';
  }
}
