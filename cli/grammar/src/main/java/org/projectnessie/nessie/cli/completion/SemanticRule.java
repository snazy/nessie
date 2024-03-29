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

import com.google.errorprone.annotations.CanIgnoreReturnValue;
import java.util.List;
import java.util.function.Function;
import org.immutables.value.Value;

/** Provides a callback for a rule that is used to provide suggestions. */
@Value.Immutable
public interface SemanticRule {
  int ruleIndex();

  Function<String, List<String>> suggestions();

  static Builder builder() {
    return ImmutableSemanticRule.builder();
  }

  static SemanticRule semanticRule(int ruleIndex, Function<String, List<String>> suggestions) {
    return ImmutableSemanticRule.of(ruleIndex, suggestions);
  }

  interface Builder {

    @CanIgnoreReturnValue
    Builder ruleIndex(int ruleIndex);

    @CanIgnoreReturnValue
    Builder suggestions(Function<String, List<String>> suggestions);

    SemanticRule build();
  }
}
