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

import static org.projectnessie.nessie.cli.completion.CompletionTestCase.fromGrammar;
import static org.projectnessie.nessie.cli.completion.RecoveryRule.recoveryRule;

import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.stream.Stream;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

// import org.projectnessie.nessie.cli.completion.test.JavaLexer;
// import org.projectnessie.nessie.cli.completion.test.JavaParser;

public class TestAntlrCompleter extends BaseTestAntlrCompletion {

  @ParameterizedTest
  @MethodSource
  public void general(CompletionTestCase testCase) throws Exception {
    test(testCase);
  }

  public static Stream<CompletionTestCase> general() {
    return Stream.of(
        fromGrammar("r: A A 'B'; A: 'A';").input("AA").expect("B"),
        fromGrammar("r: 'A' ('B'|'C'|'D') EOF;")
            .input("A")
            .expect("B")
            .expect("C")
            .expect("D"), // TODO test also with optionals, and optional rules
        fromGrammar("r: A (A|C) EOF; A: 'A'; B: 'B'; C: 'C';")
            .input("A")
            .expect("A")
            .expect("C"), // This tests split interval sets
        fromGrammar("r: 'A' EOF;").input("A").expect("EOF"),

        // Optionals
        fromGrammar("r: 'A' 'B'? 'C' EOF;").input("A").expect("B").expect("C"),
        fromGrammar("r: 'A' w? 'C'; w: 'B' 'W'; ").input("A").expect("B", "r", "w").expect("C"),
        fromGrammar("r: 'A' ('B'|) 'C' EOF;")
            .input("A")
            .expect("B")
            .expect("C"), // Implicit optional
        fromGrammar("r: A+ B; A: 'A'; B:'B';").input("A").expect("A").expect("B"),
        fromGrammar("r: w+ B; w: A 'C'?;  A: 'A'; B:'B';")
            .input("A")
            .expect("C", "r", "w")
            .expect("A", "r", "w")
            .expect("B"),
        fromGrammar("r: A* B; A: 'A'; B:'B';").input("A").expect("A").expect("B"),
        // Fun fact: When it's greedy the transitions are ordered different (probably because of the
        // priority change and antlr4 might prioritize
        // transitions by their order)
        fromGrammar("r: A+? B; A: 'A'; B:'B';").input("A").expect("B").expect("A"),
        fromGrammar("r: A*? B; A: 'A'; B:'B';").input("A").expect("B").expect("A"),
        fromGrammar("r: 'A' 'B'?? 'C' EOF;").input("A").expect("C").expect("B"),
        fromGrammar("r: 'A'+? ('B'|'C') EOF;").input("AAAAA").expect("B").expect("C").expect("A"));
  }

  @ParameterizedTest
  @MethodSource
  public void removesDuplicatedTokens(CompletionTestCase testCase) throws Exception {
    test(testCase);
  }

  public static Stream<CompletionTestCase> removesDuplicatedTokens() {
    return Stream.of(fromGrammar("r: A | . | A; A: 'A';").input("").expect("A"));
  }

  @ParameterizedTest
  @MethodSource
  public void nonSetTransitions(CompletionTestCase testCase) throws Exception {
    test(testCase);
  }

  public static Stream<CompletionTestCase> nonSetTransitions() {
    return Stream.of(
        fromGrammar("r:  ~A; A: 'A'; B: 'B'; C: 'C';").input("").expect("B").expect("C"),
        fromGrammar("r:  ~A B; A: 'A'; B: 'B'; C: 'C';").input("C").expect("B"),
        fromGrammar("r:  ~(A | B); A: 'A'; B: 'B'; C: 'C';").input("").expect("C"),
        fromGrammar("r:  (~(A | C)); A: 'A'; B: 'B'; C: 'C';").input("").expect("B"),
        fromGrammar("r:  (~(A | B | C)); A: 'A'; B: 'B'; C: 'C';").input(""));
  }

  @ParameterizedTest
  @MethodSource
  public void followSubrules(CompletionTestCase testCase) throws Exception {
    test(testCase);
  }

  public static Stream<CompletionTestCase> followSubrules() {
    return Stream.of(
        fromGrammar(
                """
     first: A second;
     second: B;
     A: 'A';
     B: 'B';
     """)
            .input("A")
            .expect("B", "first", "second"),
        fromGrammar(
                """
     first: A second C;
     second: B;
     A: 'A';
     B: 'B';
     C: 'C';
     """)
            .input("AB")
            .expect("C", "first"),
        fromGrammar(
                """
     first: A second D;
     second: B;
     third: C second B;

     A: 'A';
     B: 'B';
     C: 'C';
     D: 'D';""")
            .input("AB")
            .expect("D", "first"));
  }

  @ParameterizedTest
  @MethodSource
  public void leftRecursion(CompletionTestCase testCase) throws Exception {
    test(testCase);
  }

  public static Stream<CompletionTestCase> leftRecursion() {
    String grammar =
        """
    expr: expr (MULT|DIV) expr
      | expr (PLUS|MINUS) expr
      | ID;

    literal: ID;

    WS: [\\p{White_Space}] -> skip;
    ID: [a-zA-Z0-9]+;
    MULT: '*';
    DIV: '/';
    PLUS: '+';
    MINUS: '-';
    """;

    return Stream.of(
        fromGrammar(grammar)
            .input("a + b")
            .expect("MULT", List.of(List.of("expr", "expr"), List.of("expr")))
            .expect("DIV", List.of(List.of("expr", "expr"), List.of("expr")))
            .expect("PLUS", "expr")
            .expect("MINUS", "expr"),
        fromGrammar(grammar).input("a +").expect("ID", "expr", "expr"),
        fromGrammar(grammar)
            .input("a + b * c / d - e")
            .expect("MULT", List.of(List.of("expr", "expr"), List.of("expr")))
            .expect("DIV", List.of(List.of("expr", "expr"), List.of("expr")))
            .expect("PLUS", "expr")
            .expect("MINUS", "expr"),
        fromGrammar(grammar)
            .input("a + b * c + e * f / v")
            .expect("MULT", List.of(List.of("expr", "expr"), List.of("expr")))
            .expect("DIV", List.of(List.of("expr", "expr"), List.of("expr")))
            .expect("PLUS", "expr")
            .expect("MINUS", "expr"));
  }

  @ParameterizedTest
  @MethodSource
  public void inlineTokensReturnTheirValue(CompletionTestCase testCase) throws Exception {
    test(testCase);
  }

  public static Stream<CompletionTestCase> inlineTokensReturnTheirValue() {
    return Stream.of(fromGrammar("r: 'A';").input("A"));
  }

  @ParameterizedTest
  @MethodSource
  public void dots(CompletionTestCase testCase) throws Exception {
    test(testCase);
  }

  public static Stream<CompletionTestCase> dots() {
    return Stream.of(
        fromGrammar("r: .; A: [a-zA-Z0-9]; B: 'b';").input("").expect("A").expect("B"));
  }

  @ParameterizedTest
  @MethodSource
  public void ignoresTokensInOtherChannels(CompletionTestCase testCase) throws Exception {
    test(testCase);
  }

  public static Stream<CompletionTestCase> ignoresTokensInOtherChannels() {
    return Stream.of(
        fromGrammar(
                """
                channels {POTATO}

                A:'a' -> channel(HIDDEN);
                B: 'b' -> channel(POTATO);
                C:'c';
                """,
                "r: .;")
            .input("")
            .ignoreNonDefaultChannels(true)
            .expect("C"),
        fromGrammar(
                """
                channels {POTATO}

                A: 'a' -> channel(HIDDEN);
                B: 'b' -> channel(POTATO);
                C:'c';
                """,
                "r: .;")
            .input("")
            .expect("A")
            .expect("B")
            .expect("C"),
        fromGrammar(
                """
                channels {POTATO}

                A: 'a' -> channel(0);
                B: 'b';
                """,
                "r: .;")
            .input("")
            .expect("A")
            .expect("B"));
  }

  @ParameterizedTest
  @MethodSource
  public void ignoreSuggestionsInNonDefaultChannels(CompletionTestCase testCase) throws Exception {
    test(testCase);
  }

  public static Stream<CompletionTestCase> ignoreSuggestionsInNonDefaultChannels() {
    return Stream.of(
        fromGrammar(
                """
                A: 'A' -> mode(OTHER_MODE);
                mode OTHER_MODE;
                A2: 'B' -> mode(DEFAULT_MODE);
                """,
                "r: .+;")
            .input("")
            .expect("A")
            .expect("A2"),

        // Technically here "A2" is the only valid suggestion. I'm not sure it's worth to try to fix
        // it
        // After all, "." is probably not used
        fromGrammar(
                """
                A: 'A' -> mode(OTHER_MODE);
                mode OTHER_MODE;
                A2: 'B' -> mode(DEFAULT_MODE);
                """,
                "r: .+;")
            .input("ABABABA")
            .expect("A")
            .expect("A2"));
  }

  @ParameterizedTest
  @MethodSource
  public void considerTokenTypes(CompletionTestCase testCase) throws Exception {
    test(testCase);
  }

  public static Stream<CompletionTestCase> considerTokenTypes() {
    return Stream.of(
        fromGrammar(
                """
          tokens { FOO }
          A: 'A' -> type(FOO);
          B: 'B' -> type(FOO);
          """,
                "r: FOO;")
            .input("")
            .expect("FOO"),

        // I don't think it's worth the effort to try to remove the "A" and "B" from here.
        // Most grammars probably don't use the dot to match anything
        fromGrammar(
                """
              tokens { FOO }
              A: 'A' -> type(FOO);
              B: 'B' -> type(FOO);
            """,
                "r: .+;")
            .input("")
            .expect("FOO")
            .expect("A")
            .expect("B"));
  }

  @ParameterizedTest
  @MethodSource
  public void differentStartingRule(CompletionTestCase testCase) throws Exception {
    test(testCase);
  }

  public static Stream<CompletionTestCase> differentStartingRule() {
    String grammar =
        """
    first2: 'A';
    second2: B first2;
    B: 'B';
    """;

    return Stream.of(
        fromGrammar(grammar).input("").expect("A", "first2"),
        fromGrammar(grammar).input("").startingRule("second2").expect("B", "second2"));
  }

  @ParameterizedTest
  @MethodSource
  public void nonExistingStartingRule(CompletionTestCase testCase) throws Exception {
    test(testCase);
  }

  public static Stream<CompletionTestCase> nonExistingStartingRule() {
    // TODO

    //        String grammar ="""
    //      first2: 'A';
    //      second2: B first2;
    //      B: 'B';
    //    """).input( "").startingAtRule(() => 3);
    return Stream.of(
        //        await expect(() => base.thenExpect(['B'])).rejects.toThrow("Unexpected starting
        // rule: 3");
        );
  }

  @ParameterizedTest
  @MethodSource
  public void withContext(CompletionTestCase testCase) throws Exception {
    test(testCase);
  }

  public static Stream<CompletionTestCase> withContext() {
    String grammar =
        """
      first: 'A' second fourth;
      second: 'A' third;
      third: A;
      fourth: 'A';
      A: 'A';
      """;
    return Stream.of(
        fromGrammar(grammar).input("A").expect("A", "first", "second"),
        fromGrammar(grammar).input("AA").expect("A", "first", "second", "third"),
        fromGrammar(grammar).input("AAA").expect("A", "first", "fourth"));
  }

  @ParameterizedTest
  @MethodSource
  public void fusesContextInDuplicatedSuggestions(CompletionTestCase testCase) throws Exception {
    test(testCase);
  }

  public static Stream<CompletionTestCase> fusesContextInDuplicatedSuggestions() {
    return Stream.of(
        fromGrammar(
                """

          first: second | third;
          second: A;
          third: A? fourth;
          fourth: A;
          A: 'A';
          """)
            .input("")
            .expect(
                "A",
                List.of(
                    List.of("first", "second"),
                    List.of("first", "third"),
                    List.of("first", "third", "fourth"))));
  }

  @ParameterizedTest
  @MethodSource
  public void findsRules(CompletionTestCase testCase) throws Exception {
    test(testCase);
  }

  public static Stream<CompletionTestCase> findsRules() {
    String grammar =
        """
        first: expr PLUS expr;
        expr: 'A' another? notThisOne?;
        another: 'B';
        notThisOne: 'C';
        PLUS: '+';
        A: 'A';
        """;

    return Stream.of(
        fromGrammar(grammar)
            .input("")
            .withSuggestRules(p -> Set.of(p.getRuleIndex("expr"), p.getRuleIndex("another")))
            .expectRule("expr", "first"),
        fromGrammar(grammar)
            .input("")
            .withSuggestRules(p -> Set.of(p.getRuleIndex("another")))
            .expect("A", "first", "expr"),
        fromGrammar(grammar)
            .input("A")
            .withSuggestRules(p -> Set.of(p.getRuleIndex("expr"), p.getRuleIndex("another")))
            .expectRule(
                "another",
                List.of(List.of("first", "expr"), List.of("first", "expr", "notThisOne")))
            .expect("C", "first", "expr")
            .expect("PLUS", "first"));
  }

  @ParameterizedTest
  @MethodSource
  public void basicRecovery(CompletionTestCase testCase) throws Exception {
    test(testCase);
  }

  public static Stream<CompletionTestCase> basicRecovery() {
    String grammar =
        """
      expression: assignment | simpleExpression;

      assignment: (VAR | LET) ID EQUAL simpleExpression;

      simpleExpression
          : simpleExpression (PLUS | MINUS) simpleExpression
          | simpleExpression (MULTIPLY | DIVIDE) simpleExpression
          | variableRef
          | functionRef
      ;

      variableRef: ID;
      functionRef: ID OPEN_PAR CLOSE_PAR;

      VAR: [vV] [aA] [rR];
      LET: [lL] [eE] [tT];

      PLUS: '+';
      MINUS: '-';
      MULTIPLY: '*';
      DIVIDE: '/';
      EQUAL: '=';
      OPEN_PAR: '(';
      CLOSE_PAR: ')';
      ID: [a-zA-Z] [a-zA-Z0-9_]*;
      WS: [ \\n\\r\\t] -> channel(HIDDEN);
      """;

    return Stream.of(
        fromGrammar(grammar)
            .withRecovery(
                (parser) -> {
                  int assignmentRule = parser.getRuleIndex("assignment");
                  return List.of(
                      recoveryRule(
                          assignmentRule,
                          parser.getTokenType("VAR"),
                          assignmentRule,
                          false,
                          false));
                })
            .input("let = = var a =")
            .expect(
                "ID",
                List.of(
                    List.of("expression", "simpleExpression", "functionRef"),
                    List.of("expression", "simpleExpression", "variableRef"))),
        fromGrammar(grammar)
            .withRecovery(
                (parser) -> {
                  int assignmentRule = parser.getRuleIndex("assignment");
                  return List.of(
                      recoveryRule(
                          assignmentRule,
                          parser.getTokenType("VAR"),
                          assignmentRule,
                          false,
                          false));
                })
            .input("let a = b")
            .expect("OPEN_PAR", "expression", "assignment", "simpleExpression", "functionRef")
            .expect("PLUS", "expression", "assignment", "simpleExpression")
            .expect("MINUS", "expression", "assignment", "simpleExpression")
            .expect("MULTIPLY", "expression", "assignment", "simpleExpression")
            .expect("DIVIDE", "expression", "assignment", "simpleExpression"),
        // TODO this as well??
        //        const debugStats = await recoveryBase.whenInput("let = = var a =
        // b").thenExpect(["PLUS", "MINUS", "MULTIPLY", "DIVIDE", "OPEN_PAR"]);
        //        expect(Object.values(debugStats._recoveries).length).toBe(1);
        //        expect(Object.values(debugStats._recoveries)[0].attempts).toBe(1);

        // This test is useful to check that if 'andFindToken' is the same token that
        // begins the rule in 'ifInRule' it skips it and doesn't try to recover from
        // the original token (which would be useless)

        fromGrammar(grammar)
            .withRecovery(
                (parser) -> {
                  int assignmentRule = parser.getRuleIndex("assignment");
                  return Collections.singletonList(
                      recoveryRule(
                          assignmentRule,
                          parser.getTokenType("LET"),
                          assignmentRule,
                          false,
                          false));
                })
            .input("let = = let a = b")
            .expect("OPEN_PAR", "expression", "simpleExpression", "functionRef")
            .expect("PLUS", "expression", "simpleExpression")
            .expect("MINUS", "expression", "simpleExpression")
            .expect("MULTIPLY", "expression", "simpleExpression")
            .expect("DIVIDE", "expression", "simpleExpression"));
  }

  @ParameterizedTest
  @MethodSource
  public void tooManyRecoveries(CompletionTestCase testCase) throws Exception {
    test(testCase);
  }

  public static Stream<CompletionTestCase> tooManyRecoveries() {
    String grammar =
        """
      expression: (assignment | simpleExpression)+ EOF;

      assignment: LET ID EQUAL simpleExpression;

      simpleExpression
          : simpleExpression (PLUS | MINUS) simpleExpression
          | simpleExpression (MULTIPLY | DIVIDE) simpleExpression
          | variableRef;
      variableRef: ID;

      LET: [lL] [eE] [tT];

      PLUS: '+';
      EQUAL: '=';
      MULTIPLY: '*';
      DIVIDE: '/';
      MINUS: '-';
      ID: [a-zA-Z] [a-zA-Z0-9_]*;
      WS: [ \\n\\r\\t] -> channel(HIDDEN);
      """;
    return Stream.of(
        fromGrammar(grammar)
            //      .withRecovery((parser) => {
            //            const foo = {ifInRule: parser.RULE_assignment, andFindToken: parser.LET,
            // thenFinishRule: true};
            //            return [foo];
            //        })
            .input("let a = b let b = b let = = let a = b")
            .expect("PLUS")
            .expect("MINUS")
            .expect("MULTIPLY")
            .expect("DIVIDE")
            .expect("LET")
            .expect("ID")
            .expect("EOF")
        //      expect(Object.values(debugStats._recoveries).length).toBe(1);
        //    expect(Object.values(debugStats._recoveries)[0].attempts).toBe(1);
        );
  }
  /*
    @ParameterizedTest
    @MethodSource
    public void recoveryWithRepetition(CompletionTestCase testCase) throws Exception {
      test(testCase);
    }

    public static Stream<CompletionTestCase> recoveryWithRepetition() {
      return Stream.of(
          fromParserLexer(JavaParser.class, JavaLexer.class)
              .withRecovery(
                  parser -> {
                    int blockStatement = parser.getRuleIndex("blockStatement");
                    return Collections.singletonList(
                        recoveryRule(blockStatement, parser.getTokenType("SEMI"), -1, true, true));
                  })
              .input(
                  """
            class HelloWorld {
            public static void main(String[] args) {
              System.out.println("foo";
            }

            public static void main
            """)
              .expect(
                  "LPAREN",
                  "compilationUnit",
                  "typeDeclaration",
                  "classDeclaration",
                  "classBody",
                  "classBodyDeclaration",
                  "memberDeclaration",
                  "methodDeclaration",
                  "formalParameters"),
          fromParserLexer(JavaParser.class, JavaLexer.class)
              .withRecovery(
                  parser -> {
                    int blockStatement = parser.getRuleIndex("blockStatement");
                    return Collections.singletonList(
                        recoveryRule(blockStatement, parser.getTokenType("SEMI"), -1, true, true));
                  })
              .input(
                  """
            class HelloWorld {
                  public static void main(String[] args) {
                    System.out.println("foo");
                  }

                  public static void main
            """)
              .expect(
                  "LPAREN",
                  "compilationUnit",
                  "typeDeclaration",
                  "classDeclaration",
                  "classBody",
                  "classBodyDeclaration",
                  "memberDeclaration",
                  "methodDeclaration",
                  "formalParameters"));
    }

    @ParameterizedTest
    @MethodSource
    public void javaGrammar(CompletionTestCase testCase) throws Exception {
      test(testCase);
    }

    public static Stream<CompletionTestCase> javaGrammar() {
      CompletionTestCase case1 =
          fromParserLexer(JavaParser.class, JavaLexer.class)
              .input(
                  """
                  class HelloWorld {
                        public static void main(String[] args) {
                            System.out.println("foo"
                  """);
      List.of(
              "MUL",
              "DIV",
              "MOD",
              "ADD",
              "SUB",
              "GT",
              "LT",
              "LE",
              "GE",
              "EQUAL",
              "NOTEQUAL",
              "BITAND",
              "CARET",
              "BITOR",
              "AND",
              "OR",
              "QUESTION",
              "ASSIGN",
              "ADD_ASSIGN",
              "SUB_ASSIGN",
              "MUL_ASSIGN",
              "DIV_ASSIGN",
              "AND_ASSIGN",
              "OR_ASSIGN",
              "XOR_ASSIGN",
              "MOD_ASSIGN",
              "LSHIFT_ASSIGN",
              "RSHIFT_ASSIGN",
              "URSHIFT_ASSIGN",
              "LBRACK",
              "DOT",
              "COLONCOLON",
              "INC",
              "DEC",
              "INSTANCEOF")
          .forEach(
              label ->
                  case1.expect(
                      label,
                      "compilationUnit",
                      "typeDeclaration",
                      "classDeclaration",
                      "classBody",
                      "classBodyDeclaration",
                      "memberDeclaration",
                      "methodDeclaration",
                      "methodBody",
                      "block",
                      "blockStatement",
                      "statement",
                      "expression",
                      "methodCall",
                      "arguments",
                      "expressionList",
                      "expression"));

      case1.expect(
          "COMMA",
          "compilationUnit",
          "typeDeclaration",
          "classDeclaration",
          "classBody",
          "classBodyDeclaration",
          "memberDeclaration",
          "methodDeclaration",
          "methodBody",
          "block",
          "blockStatement",
          "statement",
          "expression",
          "methodCall",
          "arguments",
          "expressionList");
      case1.expect(
          "RPAREN",
          "compilationUnit",
          "typeDeclaration",
          "classDeclaration",
          "classBody",
          "classBodyDeclaration",
          "memberDeclaration",
          "methodDeclaration",
          "methodBody",
          "block",
          "blockStatement",
          "statement",
          "expression",
          "methodCall",
          "arguments");

      return Stream.of(case1);
    }
  */
}
