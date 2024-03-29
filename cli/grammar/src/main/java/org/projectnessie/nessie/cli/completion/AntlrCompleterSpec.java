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
import java.util.Set;
import java.util.function.Function;
import org.immutables.value.Value;

@Value.Immutable
public interface AntlrCompleterSpec {
  List<SemanticRule> semanticRules();

  Set<Integer> suggestRules();

  Set<Integer> ignoredTokens();

  List<RecoveryRule> recoveryRules();

  @Value.Default
  default boolean ignoreNonDefaultChannels() {
    return false;
  }

  @Value.NonAttribute
  default AntlrCompleter newCompleter() {
    return new AntlrCompleterImpl(this);
  }

  static Builder builder() {
    return ImmutableAntlrCompleterSpec.builder();
  }

  static AntlrCompleterSpec antlrCompleterSpec(
      List<SemanticRule> semanticRules,
      Set<Integer> preferredRules,
      Set<Integer> ignoredTokens,
      List<RecoveryRule> recoveryRules,
      boolean ignoreNonDefaultChannels) {
    return ImmutableAntlrCompleterSpec.of(
        semanticRules, preferredRules, ignoredTokens, recoveryRules, ignoreNonDefaultChannels);
  }

  interface Builder {

    @CanIgnoreReturnValue
    Builder from(AntlrCompleterSpec instance);

    @CanIgnoreReturnValue
    Builder ignoreNonDefaultChannels(boolean ignoreNonDefaultChannels);

    @CanIgnoreReturnValue
    Builder addRecoveryRule(RecoveryRule element);

    @CanIgnoreReturnValue
    Builder addRecoveryRules(RecoveryRule... elements);

    @CanIgnoreReturnValue
    Builder recoveryRules(Iterable<? extends RecoveryRule> elements);

    @CanIgnoreReturnValue
    Builder addAllRecoveryRules(Iterable<? extends RecoveryRule> elements);

    @CanIgnoreReturnValue
    Builder addSemanticRule(SemanticRule element);

    @CanIgnoreReturnValue
    Builder addSemanticRule(int ruleIndex, Function<String, List<String>> suggestions);

    @CanIgnoreReturnValue
    Builder addSemanticRules(SemanticRule... elements);

    @CanIgnoreReturnValue
    Builder semanticRules(Iterable<? extends SemanticRule> elements);

    @CanIgnoreReturnValue
    Builder addAllSemanticRules(Iterable<? extends SemanticRule> elements);

    @CanIgnoreReturnValue
    Builder addSuggestRule(int element);

    @CanIgnoreReturnValue
    Builder addSuggestRules(int... elements);

    @CanIgnoreReturnValue
    Builder suggestRules(Iterable<Integer> elements);

    @CanIgnoreReturnValue
    Builder addAllSuggestRules(Iterable<Integer> elements);

    @CanIgnoreReturnValue
    Builder addIgnoredToken(int element);

    @CanIgnoreReturnValue
    Builder addIgnoredTokens(int... elements);

    @CanIgnoreReturnValue
    Builder ignoredTokens(Iterable<Integer> elements);

    @CanIgnoreReturnValue
    Builder addAllIgnoredTokens(Iterable<Integer> elements);

    AntlrCompleterSpec build();
  }
}
