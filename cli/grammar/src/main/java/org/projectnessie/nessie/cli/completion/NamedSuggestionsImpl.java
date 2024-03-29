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
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import org.antlr.v4.runtime.Vocabulary;

final class NamedSuggestionsImpl implements NamedSuggestions {
  private final Suggestion[] suggestions;
  private final Vocabulary vocabulary;
  private final String[] ruleNames;

  NamedSuggestionsImpl(
      Collection<Suggestion> suggestions, Vocabulary vocabulary, String[] ruleNames) {
    this.suggestions = suggestions.toArray(new Suggestion[0]);
    this.vocabulary = vocabulary;
    this.ruleNames = ruleNames;
  }

  @Override
  public List<NamedSuggestion> suggestions() {
    return Arrays.stream(suggestions)
        .map(suggestion -> (NamedSuggestion) new NamedSuggestionImpl(suggestion))
        .toList();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;

    NamedSuggestionsImpl that = (NamedSuggestionsImpl) o;
    return Arrays.equals(suggestions, that.suggestions)
        // intentional object identity
        && vocabulary == that.vocabulary
        && ruleNames == that.ruleNames;
  }

  @Override
  public int hashCode() {
    return Arrays.hashCode(suggestions);
  }

  @Override
  public String toString() {
    return "NamedSuggestionsImpl{" + suggestions() + '}';
  }

  private class NamedSuggestionImpl implements NamedSuggestion {
    final Suggestion suggestion;

    private NamedSuggestionImpl(Suggestion suggestion) {
      this.suggestion = suggestion;
    }

    @Override
    public boolean isRule() {
      return suggestion.isRule;
    }

    @Override
    public Name name() {
      return new Name() {
        @Override
        public int index() {
          return suggestion.id;
        }

        @Override
        public String symbolicName() {
          if (suggestion.isRule) {
            return ruleNames[suggestion.id];
          }

          String symbolic = vocabulary.getSymbolicName(suggestion.id);
          if (symbolic != null) {
            return symbolic;
          }
          return displayName();
        }

        @Override
        public String displayName() {
          if (suggestion.isRule) {
            return ruleNames[suggestion.id];
          }

          return trimQuotes(vocabulary.getDisplayName(suggestion.id));
        }

        @Override
        public String toString() {
          return displayName();
        }

        @Override
        public int hashCode() {
          return suggestion.id;
        }

        @Override
        public boolean equals(Object obj) {
          if (!(obj instanceof Name)) {
            return false;
          }
          return suggestion.id == ((Name) obj).index();
        }
      };
    }

    @Override
    public List<List<Rule>> ctxt() {
      return suggestion.ctx.stream()
          .map(
              ruleList -> {
                int size = ruleList.size();
                List<Rule> list = new ArrayList<>(size);
                for (int rule : ruleList) {
                  list.add(new RuleImpl(rule));
                }
                return list;
              })
          .toList();
    }

    @Override
    public String toString() {
      return "NamedSuggestion{" + (isRule() ? "rule " : "label ") + name() + ", " + ctxt() + '}';
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;

      NamedSuggestionImpl that = (NamedSuggestionImpl) o;
      return Objects.equals(suggestion, that.suggestion);
    }

    @Override
    public int hashCode() {
      return Objects.hashCode(suggestion);
    }
  }

  private final class RuleImpl implements Rule {
    final int index;

    private RuleImpl(int index) {
      this.index = index;
    }

    @Override
    public String name() {
      return ruleNames[index];
    }

    @Override
    public int ruleIndex() {
      return index;
    }

    @Override
    public String toString() {
      return "Rule{" + +index + ',' + name() + '}';
    }
  }

  private static String trimQuotes(String s) {
    int start = s.charAt(0) == '\'' ? 1 : 0;
    int len = s.length();
    int len1 = len - 1;
    int end = s.charAt(len1) == '\'' ? len1 : len;
    if (start == 0 && end == len) {
      // It is actually not possible to have an unquoted label, but better be safe than sorry.
      return s;
    }
    return s.substring(start, end);
  }
}
