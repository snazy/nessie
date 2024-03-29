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

import static java.nio.file.Files.writeString;
import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThat;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import org.antlr.v4.Tool;
import org.antlr.v4.runtime.BufferedTokenStream;
import org.antlr.v4.runtime.CharStream;
import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.Lexer;
import org.antlr.v4.runtime.Parser;
import org.antlr.v4.runtime.TokenStream;
import org.antlr.v4.tool.Grammar;
import org.antlr.v4.tool.ast.GrammarRootAST;
import org.assertj.core.api.SoftAssertions;
import org.assertj.core.api.junit.jupiter.InjectSoftAssertions;
import org.assertj.core.api.junit.jupiter.SoftAssertionsExtension;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;

@ExtendWith(SoftAssertionsExtension.class)
public abstract class BaseTestAntlrCompletion {
  @InjectSoftAssertions protected SoftAssertions soft;

  @TempDir Path tempDir;

  protected void test(CompletionTestCase testCase) throws Exception {
    Parser parser;
    Lexer lexer;
    CharStream input = CharStreams.fromString(testCase.input());
    if (testCase.grammarSource() != null) {
      Path grammarFile = tempDir.resolve("Grammar.g4");
      writeString(grammarFile, "grammar Grammar;\n\n" + testCase.grammarSource());

      Tool tool = new Tool();
      Grammar grammar = tool.loadGrammar(grammarFile.toString());

      assertThat(tool.getNumErrors()).describedAs("Test grammar issue").isEqualTo(0);

      lexer = grammar.createLexerInterpreter(input);
      TokenStream tokenSource = new BufferedTokenStream(lexer);
      parser = grammar.createParserInterpreter(tokenSource);
    } else if (testCase.lexerSource() != null) {
      Path lexerFile = tempDir.resolve("GrammarLexer.g4");
      writeString(
          lexerFile,
          """
              lexer grammar GrammarLexer;

              """
              + testCase.lexerSource());
      Path parserFile = tempDir.resolve("GrammarParser.g4");
      writeString(
          parserFile,
          """
              parser grammar GrammarParser;

              options {
                tokenVocab = GrammarLexer;
              }

              """
              + testCase.parserSource());

      Tool tool = new Tool();
      Grammar parserGrammar = null;
      Grammar lexerGrammar = null;
      List<GrammarRootAST> sortedGrammars =
          tool.sortGrammarByTokenVocab(List.of(lexerFile.toString(), parserFile.toString()));
      for (GrammarRootAST t : sortedGrammars) {
        Grammar g = tool.createGrammar(t);
        g.fileName = t.fileName;

        tool.process(g, true);

        if (t.fileName.endsWith("Parser.g4")) {
          assertThat(tool.getNumErrors())
              .describedAs("Test grammar issue in parser source")
              .isEqualTo(0);
          parserGrammar = g;
        }
        if (t.fileName.endsWith("Lexer.g4")) {
          assertThat(tool.getNumErrors())
              .describedAs("Test grammar issue in lexer source")
              .isEqualTo(0);
          lexerGrammar = g;
        }
      }

      lexer = requireNonNull(lexerGrammar).createLexerInterpreter(input);
      TokenStream tokenSource = new BufferedTokenStream(lexer);
      parser = requireNonNull(parserGrammar).createParserInterpreter(tokenSource);
    } else {
      lexer = testCase.lexerType().getDeclaredConstructor(CharStream.class).newInstance(input);
      TokenStream tokenSource = new BufferedTokenStream(lexer);
      parser =
          testCase.parserType().getDeclaredConstructor(TokenStream.class).newInstance(tokenSource);
    }

    int ruleIndex = 0;
    if (testCase.startingRule() != null) {
      ruleIndex = parser.getRuleIndexMap().get(testCase.startingRule());
    }
    if (testCase.startingRuleInt() >= 0) {
      ruleIndex = testCase.startingRuleInt();
    }

    AntlrCompleterSpec spec =
        AntlrCompleterSpec.builder()
            .ignoredTokens(testCase.ignoredTokens())
            .recoveryRules(testCase.recoveryRulesBuilder().apply(parser))
            .ignoreNonDefaultChannels(testCase.ignoreNonDefaultChannels())
            .suggestRules(testCase.suggestRulesBuilder().apply(parser))
            .build();

    AntlrCompleter completer = spec.newCompleter();
    int caretTokenIndex = 0;
    NamedSuggestions suggestions = completer.suggest(lexer, parser, ruleIndex, caretTokenIndex);

    List<Expect> suggest = new ArrayList<>();
    for (NamedSuggestion named : suggestions.suggestions()) {
      suggest.add(
          new Expect(
              named.name().symbolicName(),
              named.ctxt().stream().map(rules -> rules.stream().map(Rule::name).toList()).toList(),
              named.isRule()));
    }

    if (testCase.expected().isEmpty()) {
      soft.assertThat(suggest).isEmpty();
    } else {
      soft.assertThat(suggest).containsExactlyElementsOf(testCase.expected());
    }
  }
}
