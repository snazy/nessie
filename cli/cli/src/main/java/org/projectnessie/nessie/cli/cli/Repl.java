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
package org.projectnessie.nessie.cli.cli;

import io.quarkus.picocli.runtime.annotations.TopCommand;
import java.util.concurrent.Callable;
import org.jline.reader.Completer;
import org.jline.reader.EndOfFileException;
import org.jline.reader.LineReader;
import org.jline.reader.LineReaderBuilder;
import org.jline.reader.Parser;
import org.jline.reader.UserInterruptException;
import org.jline.terminal.Terminal;
import org.jline.terminal.TerminalBuilder;
import org.jline.utils.OSUtils;
import picocli.CommandLine;

@TopCommand
@CommandLine.Command
public class Repl implements Callable<Integer> {
  @Override
  public Integer call() throws Exception {

    Terminal terminal = TerminalBuilder.builder().build();

    System.out.printf(
        """
          Terminal     : %s
                   size: %s
                   type: %s
                   attr: %s
            buffer size: %s
                palette: %s
          """,
        terminal.getWidth(),
        terminal.getHeight(),
        terminal.getName(),
        terminal.getSize(),
        terminal.getType(),
        terminal.getAttributes(),
        terminal.getBufferSize(),
        terminal.getPalette());

    Completer completer;
    Parser parser;
    LineReader reader =
        LineReaderBuilder.builder()
            .terminal(terminal)
            // .completer(completer)
            // .parser(parser)
            // .highlighter(highlighter)
            .variable(LineReader.SECONDARY_PROMPT_PATTERN, "%M%P > ")
            .variable(LineReader.INDENTATION, 2)
            .variable(LineReader.LIST_MAX, 100)
            // .variable(LineReader.HISTORY_FILE, Paths.get(root, "history"))
            .option(LineReader.Option.INSERT_BRACKET, true)
            .option(LineReader.Option.EMPTY_WORD_OPTIONS, false)
            .option(
                LineReader.Option.USE_FORWARD_SLASH,
                true) // use forward slash in directory separator
            .option(LineReader.Option.DISABLE_EVENT_EXPANSION, true)
            .build();
    if (OSUtils.IS_WINDOWS) {
      reader.setVariable(
          LineReader.BLINK_MATCHING_PAREN,
          0); // if enabled cursor remains in begin parenthesis (gitbash)
    }

    while (true) {
      String line;
      try {
        line = reader.readLine("Nessie> ");
      } catch (EndOfFileException | UserInterruptException e) {
        break;
      }
      System.err.println(line);
    }

    return 0;
  }
}
