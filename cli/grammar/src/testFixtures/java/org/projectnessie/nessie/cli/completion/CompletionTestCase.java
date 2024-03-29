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

import static java.util.Collections.emptyList;
import static java.util.Collections.emptySet;
import static java.util.Objects.requireNonNull;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.Function;
import org.antlr.v4.runtime.Lexer;
import org.antlr.v4.runtime.Parser;

public class CompletionTestCase {
  private final String lexerSource;
  private final String parserSource;
  private final String grammarSource;
  private final Class<? extends Parser> parserType;
  private final Class<? extends Lexer> lexerType;
  private final Set<Integer> ignoredTokens = new HashSet<>();

  private String input;
  private boolean ignoreNonDefaultChannels;
  private String startingRule;
  private int startingRuleInt = -1;
  private final List<Expect> expected = new ArrayList<>();
  private Function<Parser, List<RecoveryRule>> recoveryRulesBuilder = p -> emptyList();
  private Function<Parser, Set<Integer>> suggestRulesBuilder = p -> emptySet();

  private CompletionTestCase(String grammarSource) {
    this.lexerSource = null;
    this.parserSource = null;
    this.grammarSource = requireNonNull(grammarSource);
    this.parserType = null;
    this.lexerType = null;
  }

  private CompletionTestCase(Class<? extends Parser> parserType, Class<? extends Lexer> lexerType) {
    this.lexerSource = null;
    this.parserSource = null;
    this.grammarSource = null;
    this.parserType = requireNonNull(parserType);
    this.lexerType = requireNonNull(lexerType);
  }

  private CompletionTestCase(String lexerSource, String parserSource) {
    this.lexerSource = requireNonNull(lexerSource);
    this.parserSource = parserSource;
    this.grammarSource = null;
    this.parserType = null;
    this.lexerType = null;
  }

  public Class<? extends Parser> parserType() {
    return parserType;
  }

  public Class<? extends Lexer> lexerType() {
    return lexerType;
  }

  public String lexerSource() {
    return lexerSource;
  }

  public String parserSource() {
    return parserSource;
  }

  public String grammarSource() {
    return grammarSource;
  }

  public String input() {
    return input;
  }

  public String startingRule() {
    return startingRule;
  }

  public int startingRuleInt() {
    return startingRuleInt;
  }

  public boolean ignoreNonDefaultChannels() {
    return ignoreNonDefaultChannels;
  }

  public Function<Parser, List<RecoveryRule>> recoveryRulesBuilder() {
    return recoveryRulesBuilder;
  }

  public Function<Parser, Set<Integer>> suggestRulesBuilder() {
    return suggestRulesBuilder;
  }

  public List<Expect> expected() {
    return expected;
  }

  public Set<Integer> ignoredTokens() {
    return ignoredTokens;
  }

  public CompletionTestCase withRecovery(Function<Parser, List<RecoveryRule>> rulesBuilder) {
    this.recoveryRulesBuilder = rulesBuilder;
    return this;
  }

  public CompletionTestCase withSuggestRules(Function<Parser, Set<Integer>> rulesBuilder) {
    this.suggestRulesBuilder = rulesBuilder;
    return this;
  }

  public CompletionTestCase input(String input) {
    this.input = input;
    return this;
  }

  public CompletionTestCase ignoreToken(int... tokens) {
    for (int token : tokens) {
      ignoredTokens.add(token);
    }
    return this;
  }

  public CompletionTestCase startingRule(String startingRule) {
    this.startingRule = startingRule;
    return this;
  }

  public CompletionTestCase startingRuleInt(int startingRuleInt) {
    this.startingRuleInt = startingRuleInt;
    return this;
  }

  public CompletionTestCase ignoreNonDefaultChannels(boolean ignoreNonDefaultChannels) {
    this.ignoreNonDefaultChannels = ignoreNonDefaultChannels;
    return this;
  }

  public CompletionTestCase expect(String label) {
    return expect(label, "r");
  }

  public CompletionTestCase expect(String label, String... rules) {
    this.expected.add(new Expect(label, List.of(List.of(rules)), false));
    return this;
  }

  public CompletionTestCase expect(String label, List<List<String>> rulesLists) {
    this.expected.add(new Expect(label, rulesLists, false));
    return this;
  }

  public CompletionTestCase expectRule(String rule, String... rules) {
    this.expected.add(new Expect(rule, List.of(List.of(rules)), true));
    return this;
  }

  public CompletionTestCase expectRule(String rule, List<List<String>> rulesLists) {
    this.expected.add(new Expect(rule, rulesLists, true));
    return this;
  }

  public static CompletionTestCase fromParserLexer(
      Class<? extends Parser> parserType, Class<? extends Lexer> lexerType) {
    return new CompletionTestCase(parserType, lexerType);
  }

  public static CompletionTestCase fromGrammar(String grammarSource) {
    return new CompletionTestCase(grammarSource);
  }

  public static CompletionTestCase fromGrammar(String lexerSource, String parserSource) {
    return new CompletionTestCase(lexerSource, parserSource);
  }

  @Override
  public String toString() {
    return "'"
        + input
        + "'"
        + ", "
        + (grammarSource != null
            ? ("grammar: " + grammarSource)
            : ("parser: " + parserType.getSimpleName()));
  }
}
