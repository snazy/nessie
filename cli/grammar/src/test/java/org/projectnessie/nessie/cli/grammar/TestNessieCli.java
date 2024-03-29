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
package org.projectnessie.nessie.cli.grammar;

public class TestNessieCli {}
/*
import static org.projectnessie.nessie.cli.completion.CompletionTestCase.fromParserLexer;

import java.util.List;
import java.util.stream.Stream;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.projectnessie.nessie.cli.completion.BaseTestAntlrCompletion;
import org.projectnessie.nessie.cli.completion.CompletionTestCase;
import org.projectnessie.nessie.cli.gr.NessieCliLexer;
import org.projectnessie.nessie.cli.gr.NessieCliParser;

public class TestNessieCli extends BaseTestAntlrCompletion {
  @ParameterizedTest
  @MethodSource
  public void nessieCompletion(CompletionTestCase testCase) throws Exception {
    test(testCase);
  }

  static Stream<CompletionTestCase> nessieCompletion() {
    return Stream.of(
        fromParserLexer(NessieCliParser.class, NessieCliLexer.class)
            .input("C")
            .expect("CREATE", "script", "statement")
            .startingRuleInt(NessieCliParser.RULE_script),
        //
        fromParserLexer(NessieCliParser.class, NessieCliLexer.class)
            .input("")
            .expect("CREATE", "script", "statement", "createReferenceStatement")
            .expect("DROP", "script", "statement", "dropReferenceStatement")
            .expect("ASSIGN", "script", "statement", "assignReferenceStatement")
            .expect("USE", "script", "statement", "useReferenceStatement")
            .expect("LIST", "script", "statement", "listReferencesStatement")
            .expect(
                "SHOW",
                List.of(
                    List.of("script", "statement", "showReferenceStatement"),
                    List.of("script", "statement", "showLogStatement")))
            .expect("MERGE", "script", "statement", "mergeBranchStatement")
            .expect("EOF", "script")
            .startingRuleInt(NessieCliParser.RULE_script),
        //
        fromParserLexer(NessieCliParser.class, NessieCliLexer.class)
            .input("CREATE")
            .expect("BRANCH", "script", "statement", "createReferenceStatement", "referenceType")
            .expect("TAG", "script", "statement", "createReferenceStatement", "referenceType")
            .startingRuleInt(NessieCliParser.RULE_script),
        //
        fromParserLexer(NessieCliParser.class, NessieCliLexer.class)
            .input("CREATE BRANCH")
            .expect("IF", "script", "statement", "createReferenceStatement", "ifNotExists")
            .expect(
                "IDENTIFIER",
                "script",
                "statement",
                "createReferenceStatement",
                "referenceName",
                "identifier")
            .expect(
                "BACKQUOTED_IDENTIFIER",
                "script",
                "statement",
                "createReferenceStatement",
                "referenceName",
                "identifier",
                "quotedIdentifier")
            .startingRuleInt(NessieCliParser.RULE_script),
        //
        fromParserLexer(NessieCliParser.class, NessieCliLexer.class)
            .ignoreToken(NessieCliLexer.EOF)
            .ignoreToken(NessieCliLexer.SEMICOLON)
            .input("CREATE BRANCH my_br")
            .expect("FROM", "script", "statement", "createReferenceStatement")
            .startingRuleInt(NessieCliParser.RULE_script),
        //
        fromParserLexer(NessieCliParser.class, NessieCliLexer.class)
            .input("SHOW")
            .expect("REFERENCE", "script", "statement", "showReferenceStatement")
            .expect("LOG", "script", "statement", "showLogStatement")
            .startingRuleInt(NessieCliParser.RULE_script));
  }
}
*/
