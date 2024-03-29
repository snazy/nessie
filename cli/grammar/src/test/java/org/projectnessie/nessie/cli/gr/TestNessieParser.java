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
package org.projectnessie.nessie.cli.gr;

import static org.junit.jupiter.params.provider.Arguments.arguments;

import java.util.List;
import java.util.stream.Stream;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.projectnessie.nessie.cli.gr.ast.Script;

public class TestNessieParser {

  @ParameterizedTest
  @MethodSource
  public void recover(String input) {
    NessieCliLexer lexer = new NessieCliLexer(input);
    NessieCliParser parser = new NessieCliParser(lexer);

    try {
      parser.Script();
    } catch (ParseException e) {
      // TODO handle "special" tokens like STRING_LITERAL + IDENTIFIER, but also know whether it's
      //  for an existing reference - so the token-type alone is not sufficient
      List<String> expectedTokenStrings =
          e.getExpectedTokenTypes().stream().map(parser::getTokenString).toList();
      throw e;
    }
  }

  static Stream<Arguments> recover() {
    return Stream.of(
        // arguments(""), // CongoCC doesn't handle empty strings well (StringIndexOOB), but single
        // space works
        arguments(" "),
        arguments("C"),
        arguments("X"),
        arguments("CREATE "),
        arguments("CREATE B"),
        arguments("CREATE X"),
        arguments("CREATE BRANCH "),
        arguments("CREATE BRANCH IF"),
        arguments("CREATE BRANCH IF NOT"),
        arguments("CREATE BRANCH IF NOT EXISTS"),
        arguments("CREATE BRANCH ;"));
  }

  @ParameterizedTest
  @MethodSource
  public void parse(String input) {
    NessieCliLexer lexer = new NessieCliLexer(input);
    NessieCliParser parser = new NessieCliParser(lexer);

    parser.Script();
    Node node = parser.rootNode();
    Script script = (Script) node;
    List<NessieStatement> statements = script.getStatements();
    NessieStatement st = statements.get(0);
    System.err.println(node);
  }

  static Stream<Arguments> parse() {
    return Stream.of(
        arguments("CREATE BRANCH \"foo\";"),
        arguments("DROP TAG bar;"),
        arguments("CREATE BRANCH if not exists foo; DROP TAG bar;"),
        arguments("create branch foo; drop tag bar;"));
  }
}
