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

import static java.util.Collections.emptySet;
import static org.projectnessie.nessie.cli.completion.AntlrSuggesterImpl.ParseState.parseState;
import static org.projectnessie.nessie.cli.completion.AntlrSuggesterImpl.ProcessStackElement.processStackElement;

import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Deque;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import org.agrona.collections.Int2ObjectHashMap;
import org.agrona.collections.IntArrayList;
import org.agrona.collections.IntHashSet;
import org.antlr.v4.runtime.CommonToken;
import org.antlr.v4.runtime.Lexer;
import org.antlr.v4.runtime.Parser;
import org.antlr.v4.runtime.Token;
import org.antlr.v4.runtime.atn.ATN;
import org.antlr.v4.runtime.atn.ATNState;
import org.antlr.v4.runtime.atn.ActionTransition;
import org.antlr.v4.runtime.atn.LexerAction;
import org.antlr.v4.runtime.atn.LexerActionType;
import org.antlr.v4.runtime.atn.LexerChannelAction;
import org.antlr.v4.runtime.atn.PrecedencePredicateTransition;
import org.antlr.v4.runtime.atn.RuleStartState;
import org.antlr.v4.runtime.atn.RuleTransition;
import org.antlr.v4.runtime.atn.Transition;
import org.antlr.v4.runtime.misc.IntegerList;
import org.antlr.v4.runtime.misc.IntervalSet;
import org.immutables.value.Value;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

final class AntlrSuggesterImpl {
  private static final Logger LOGGER = LoggerFactory.getLogger(AntlrSuggesterImpl.class);

  private static final Token CARET = new CommonToken(Token.EPSILON);
  private static final ATNState STOP =
      new ATNState() {
        @Override
        public int getStateType() {
          return -42;
        }
      };

  private final Lexer lexer;
  private final Parser parser;
  private final int ruleIndex;
  private final AntlrCompleterSpec userSpec;

  AntlrSuggesterImpl(
      @Nonnull Lexer lexer, @Nonnull Parser parser, int ruleIndex, AntlrCompleterSpec userSpec) {
    this.lexer = lexer;
    this.parser = parser;
    this.ruleIndex = ruleIndex;
    this.userSpec = userSpec;
  }

  NamedSuggestions suggest(AtomicReference<SuggestionsCache> cacheHolder) {
    List<? extends Token> all = lexer.getAllTokens();
    List<? extends Token> tokenList =
        Stream.concat(all.stream().filter(t -> t.getChannel() == 0), Stream.of(CARET)).toList();

    Int2ObjectHashMap<Set<Suggestion>> cache;

    SuggestionsCache cached = cacheHolder.get();
    if (cached != null
        && cached.atn == parser.getATN()
        && cached.ruleNames == parser.getRuleNames()) {
      LOGGER.debug("Using cached suggestions");
      cache = cached.cache;
    } else {
      LOGGER.debug(
          "Calculating cached suggestions for {} rules", parser.getATN().ruleToStartState.length);
      cache = new Int2ObjectHashMap<>();

      AntlrCompleterSpec cacheSpec =
          AntlrCompleterSpec.builder()
              .from(userSpec)
              .suggestRules(emptySet())
              .recoveryRules(emptySet())
              .build();

      for (RuleStartState rule : parser.getATN().ruleToStartState) {
        LOGGER.debug(
            "Caching suggestions for rule #{} '{}'",
            rule.ruleIndex,
            parser.getRuleNames()[rule.ruleIndex]);
        Deque<ProcessStackElement> processStack = new ArrayDeque<>();
        processStack.add(
            processStackElement(
                0, rule, new IntHashSet(), List.of(parseState(rule.ruleIndex, STOP, 0)), null));
        cache.put(rule.ruleIndex, executeATN(List.of(CARET), processStack, cacheSpec, null));
      }

      cacheHolder.compareAndSet(
          null, new SuggestionsCache(parser.getATN(), parser.getRuleNames(), cache));
    }

    RuleStartState initialState = parser.getATN().ruleToStartState[ruleIndex];
    Deque<ProcessStackElement> stack = new ArrayDeque<>();
    List<ParseState> parserStack = List.of(parseState(ruleIndex, STOP, 0));
    stack.push(processStackElement(0, initialState, new IntHashSet(), parserStack, null));

    LOGGER.debug("Calculating suggestions for input");
    Collection<Suggestion> suggestions = executeATN(tokenList, stack, userSpec, cache);

    suggestions = groupSuggestions(suggestions);
    if (userSpec.ignoreNonDefaultChannels()) {
      suggestions = filterNonDefaultChannels(suggestions);
    }

    return new NamedSuggestionsImpl(suggestions, parser.getVocabulary(), parser.getRuleNames());
  }

  private Set<Suggestion> executeATN(
      List<? extends Token> tokenList,
      Deque<ProcessStackElement> processStack,
      AntlrCompleterSpec options,
      Int2ObjectHashMap<Set<Suggestion>> cache) {
    // An iteration based approach is probably not (much) better, because we'd need a separate stack
    // with a wrapper object for the three arguments. On top, it makes debugging harder, because the
    // (natural) association of nesting to call-depth goes away.

    Set<Suggestion> suggestions = new LinkedHashSet<>();

    while (!processStack.isEmpty()) {
      ProcessStackElement elem = processStack.pop();
      int tokenStreamIndex = elem.tokenStreamIndex();
      ATNState state = elem.state();
      IntHashSet alreadyPassed = elem.alreadyPassed();
      List<ParseState> parserStack = elem.parserStack();
      RecoveryData recoveryData = elem.recoveryData();

      if (LOGGER.isDebugEnabled()) {
        Token token = tokenList.get(tokenStreamIndex);
        LOGGER.debug(
            "  Processing state #{}, rule #{} ({}) at token stream index {}: '{}'",
            state.stateNumber,
            state.ruleIndex,
            parser.getRuleNames()[state.ruleIndex],
            tokenStreamIndex,
            token.getText());
      }

      if (recoveryData != null) {
        if (recoveryData.suggestions() != suggestions.size()) {
          LOGGER.debug(
              "      not handling recovery (# of suggestions mismatch {} vs {})",
              recoveryData.suggestions(),
              suggestions.size());
          continue;
        }
        RecoveryRule rule = recoveryData.recoveryRules().get(0);
        LOGGER.debug(
            "      handling recovery for {} suggestions with rule {}",
            recoveryData.suggestions(),
            rule);
        onFail(processStack, tokenList, parserStack, tokenStreamIndex, rule, state);
        continue;
      }

      // The last state of a rule is always a `RuleStopState`
      ATNState limitNextState = null;
      if (state.getStateType() == ATNState.RULE_STOP && !parserStack.isEmpty()) {
        ParseState parserState = parserStack.get(parserStack.size() - 1);

        limitNextState = parserState.followState();
        // It's important to make a shallow copy to avoid affecting the other alternatives.
        parserStack = parserStack.subList(0, parserStack.size() - 1);
      }

      for (Transition transition : state.getTransitions()) {
        if (transition.isEpsilon() && !alreadyPassed.contains(transition.target.stateNumber)) {
          // an epsilon transition is a transition that doesn't consume any input
          if (transition.getSerializationType() == Transition.PRECEDENCE) {
            // cast is safe
            PrecedencePredicateTransition precedencePredicateTransition =
                (PrecedencePredicateTransition) transition;
            if (precedencePredicateTransition.precedence
                < parserStack.get(parserStack.size() - 1).precedence()) {
              continue;
            }
          }

          if (LOGGER.isDebugEnabled()) {
            LOGGER.debug(
                "    epsilon (ε) {} transition to {} {}",
                Transition.serializationNames.get(transition.getSerializationType()),
                ATNState.serializationNames.get(transition.target.getStateType()),
                transition.target.stateNumber);
          }

          Token nextToken = tokenList.get(tokenStreamIndex);
          if (transition.getSerializationType() == Transition.RULE) {
            @SuppressWarnings("DataFlowIssue")
            RuleTransition ruleTransition = (RuleTransition) transition;
            if (nextToken == CARET) {
              if (options.suggestRules().contains(ruleTransition.ruleIndex)) {
                addSuggestion(
                    options,
                    suggestions,
                    ruleTransition.ruleIndex,
                    rulesFromParserStack(parserStack),
                    true);
                continue;
              } else if (cache != null) {
                Set<Suggestion> cachedSuggestions = cache.get(ruleTransition.ruleIndex);
                if (cachedSuggestions != null) {
                  for (Suggestion cachedSuggestion : cachedSuggestions) {
                    addSuggestion(
                        options,
                        suggestions,
                        cachedSuggestion.id,
                        rulesFromParserStack(parserStack, cachedSuggestion.ctx.get(0)),
                        false);
                  }
                }
                continue;
              }
              // If there is no cache then it must keep going and enter the rule to find the
              // suggestions
            } else {
              if (cache != null
                  && cache.getOrDefault(ruleTransition.ruleIndex, emptySet()).stream()
                      .noneMatch(s -> s.id == nextToken.getType())) {
                // This means that the next token doesn't match any of the first possible tokens of
                // the rule. So we ignore this transition since it's going to fail either way. Plus
                // entering the rule could end  up triggering an unnecessary  recovery (since the
                // failure is guaranteed)
                continue;
              }
            }
          }

          if (limitNextState != null && transition.target != limitNextState) {
            LOGGER.debug(
                "      (limited to different state #{}, not processing)",
                limitNextState.stateNumber);
            continue;
          }

          if (transition.getSerializationType() == Transition.RULE) {
            // cast is safe
            @SuppressWarnings("DataFlowIssue")
            RuleTransition ruleTransition = (RuleTransition) transition;

            if (!options.recoveryRules().isEmpty()) {
              List<RecoveryRule> recoveryRules =
                  options.recoveryRules().stream()
                      .filter(r -> r.ifInRule() == ruleTransition.ruleIndex)
                      .toList();
              if (!recoveryRules.isEmpty()) {
                LOGGER.debug(
                    "      add recovery for rule #{} w/ {} recovery rules",
                    ruleIndex,
                    recoveryRules.size());
                processStack.add(
                    processStackElement(
                        tokenStreamIndex,
                        state,
                        alreadyPassed,
                        parserStack,
                        ImmutableRecoveryData.of(suggestions.size(), recoveryRules)));
              }
            }
          }

          List<ParseState> newParserStack = parserStack;
          if (transition.getSerializationType() == Transition.RULE) {
            // cast is safe
            @SuppressWarnings("DataFlowIssue")
            RuleTransition ruleTransition = (RuleTransition) transition;
            newParserStack = new ArrayList<>(parserStack.size() + 1);
            newParserStack.addAll(parserStack);
            newParserStack.add(
                parseState(
                    ruleTransition.ruleIndex,
                    ruleTransition.followState,
                    ruleTransition.precedence));
          }

          IntHashSet newAlreadyPassed =
              transition.getSerializationType() == Transition.RULE
                      || state.getStateType() == ATNState.RULE_STOP
                  ? new IntHashSet()
                  : prepend(alreadyPassed, transition.target.stateNumber);
          processStack.add(
              processStackElement(
                  tokenStreamIndex, transition.target, newAlreadyPassed, newParserStack, null));

          // end of "epsilon"
        } else {
          if (LOGGER.isDebugEnabled()) {
            LOGGER.debug(
                "     {} transition to {} {}",
                Transition.serializationNames.get(transition.getSerializationType()),
                ATNState.serializationNames.get(transition.target.getStateType()),
                transition.target.stateNumber);
          }
          switch (transition.getSerializationType()) {
            case Transition.SET, Transition.ATOM -> {
              Token nextToken = tokenList.get(tokenStreamIndex);
              if (nextToken == CARET) {
                IntegerList label = transition.label().toIntegerList();
                if (LOGGER.isDebugEnabled()) {
                  LOGGER.debug(
                      "      suggestions: {} -> {}",
                      label,
                      IntStream.of(label.toArray())
                          .mapToObj(l -> parser.getVocabulary().getDisplayName(l))
                          .collect(Collectors.joining(", ")));
                }
                IntArrayList rules = rulesFromParserStack(parserStack);
                for (int i = 0; i < label.size(); i++) {
                  addSuggestion(options, suggestions, label.get(i), rules, false);
                }
                // TODO collect all follow-ups from here
              } else if (transition.label().contains(nextToken.getType())) {
                // 'alreadyPassed' is now empty because it just consumed a token, so there's no
                // longer
                // a risk of getting stuck in an infinite loop.
                processStack.add(
                    processStackElement(
                        tokenStreamIndex + 1,
                        transition.target,
                        new IntHashSet(),
                        parserStack,
                        null));
              }
            }
            case Transition.NOT_SET -> {
              Token nextToken = tokenList.get(tokenStreamIndex);
              if (nextToken == CARET) {
                IntervalSet label = transition.label();
                if (LOGGER.isDebugEnabled()) {
                  LOGGER.debug(
                      "      suggestions: NOT in {} -> {}",
                      label,
                      IntStream.of(label.toArray())
                          .mapToObj(l -> parser.getVocabulary().getDisplayName(l))
                          .collect(Collectors.joining(", ")));
                }
                IntArrayList rules = rulesFromParserStack(parserStack);
                for (int i = Token.MIN_USER_TOKEN_TYPE; i <= parser.getATN().maxTokenType; i++) {
                  if (!label.contains(i)) {
                    addSuggestion(options, suggestions, i, rules, false);
                  }
                }
              } else {
                processStack.add(
                    processStackElement(
                        tokenStreamIndex + 1,
                        transition.target,
                        new IntHashSet(),
                        parserStack,
                        null));
              }
            }
            case Transition.WILDCARD -> {
              Token nextToken = tokenList.get(tokenStreamIndex);
              if (nextToken == CARET) {
                // suggestions.push(
                //    ...intervalToArray(
                //        {intervals: [{start: antlr4.Token.MIN_USER_TOKEN_TYPE, stop:
                // this.atn.maxTokenType+1}]}
                //      )
                //      .map(x => new Suggestion(x, parserStack.map(y => y[0])),
                //        parserStack)
                // );

                LOGGER.debug("      suggestions: all");
                IntArrayList rules = rulesFromParserStack(parserStack);
                for (int i = Token.MIN_USER_TOKEN_TYPE; i <= parser.getATN().maxTokenType; i++) {
                  addSuggestion(options, suggestions, i, rules, false);
                }
              } else {
                processStack.add(
                    processStackElement(
                        tokenStreamIndex + 1,
                        transition.target,
                        new IntHashSet(),
                        parserStack,
                        null));
              }
            }
              //          case Transition.ACTION -> {}
              //          case Transition.PREDICATE -> {}
              //          case Transition.PRECEDENCE -> {}
              //          case Transition.RULE -> {}
            default ->
                LOGGER.debug(
                    "      -> unhandled transition type: {}",
                    Transition.serializationNames.get(transition.getSerializationType()));
          }
        }
      }
    }

    return suggestions;
  }

  // CurrentState should always be a state with a transition that is a RuleTransition. Since this is
  // called from the state before actually entering the rule?
  private void onFail(
      Deque<ProcessStackElement> stack,
      List<? extends Token> tokenList,
      List<ParseState> parserStack,
      int tokenStreamIndex,
      RecoveryRule rule,
      ATNState state) {

    // tokenStreamIndex + 1 to avoid it from recovering in the same token, which
    // would be confusing . If you have let = = let a = b the rule starts in the
    // first 'let' so it wouldn't make any sense to try to recover by entering the
    // same rule again
    for (int i = tokenStreamIndex + 1; i < tokenList.size(); i++) {
      Token token = tokenList.get(i);
      if (token.getType() == rule.andFindToken()) {
        // TODO ? options.debugStats.recovery(rule);
        if (rule.thenGoToRule() >= 0) {
          stack.add(
              processStackElement(
                  rule.skipOne() ? i + 1 : i,
                  parser.getATN().ruleToStartState[rule.thenGoToRule()],
                  new IntHashSet(),
                  parserStack,
                  null // no recovery data
                  ));
        } else if (rule.thenFinishRule()) {
          RuleTransition ruleTransition = (RuleTransition) state.getTransitions()[0];
          stack.push(
              processStackElement(
                  rule.skipOne() ? i + 1 : i,
                  ruleTransition.followState,
                  new IntHashSet(),
                  parserStack,
                  null // no recovery data
                  ));
        }
        return;
      }
    }
  }

  private static void addSuggestion(
      AntlrCompleterSpec options,
      Set<Suggestion> suggestions,
      int i,
      IntArrayList ctx,
      boolean isRule) {
    if (!isRule && options.ignoredTokens().contains(i)) {
      LOGGER.debug("       ignoring suggestion for token #{}", i);
      return;
    }

    suggestions.add(new Suggestion(i, ctx, isRule));
  }

  private static IntArrayList rulesFromParserStack(List<ParseState> parserStack) {
    return rulesFromParserStack(parserStack, null);
  }

  private static IntArrayList rulesFromParserStack(
      List<ParseState> parserStack, IntArrayList additional) {
    int size = parserStack.size();
    IntArrayList rules =
        new IntArrayList(
            size + (additional != null ? additional.size() : 0), IntArrayList.DEFAULT_NULL_VALUE);
    for (ParseState parseState : parserStack) {
      rules.addInt(parseState.ruleIndex());
    }
    if (additional != null) {
      rules.addAll(additional);
    }
    return rules;
  }

  private static IntHashSet prepend(IntHashSet alreadyPassed, int stateNumber) {
    IntHashSet newSet = new IntHashSet((alreadyPassed.size() + 1) << 1, alreadyPassed.loadFactor());
    newSet.add(stateNumber);
    newSet.addAll(alreadyPassed);
    return newSet;
  }

  private Collection<Suggestion> filterNonDefaultChannels(Collection<Suggestion> suggestions) {
    /*
     * The problem with filtering non default channels is that neither the lexer nor the ATN leave an easy record of what is the
     * channel of a token. To access it this ends up searching for the lexer rule for that token in the lexer ATN and then looks
     * for an action state
     */
    return suggestions.stream()
        .filter(
            x -> {
              int rule = -1;
              ATN atn = lexer.getATN();
              int[] rtt = atn.ruleToTokenType;
              for (int i = 0; i < rtt.length; i++) {
                int tokenType = rtt[i];
                if (x.id == tokenType) {
                  rule = i;
                  break;
                }
              }

              // This shouldn't happen but just in case return true
              if (rule == -1) {
                return true;
              }

              boolean otherChannel = false;

              IntHashSet actions = findLexerActions(atn.ruleToStartState[rule]);
              for (int actionIndex : actions) {
                LexerAction lexerAction = atn.lexerActions[actionIndex];
                if (lexerAction.getActionType() == LexerActionType.CHANNEL) {
                  LexerChannelAction channelAction = (LexerChannelAction) lexerAction;
                  if (channelAction.getChannel() != Token.DEFAULT_CHANNEL) {
                    otherChannel = true;
                    break;
                  }
                }
              }

              return !otherChannel;
            })
        .toList();
  }

  private static IntHashSet findLexerActions(ATNState state) {
    IntHashSet collector = new IntHashSet();
    findLexerActionsRecursive(state, new IntHashSet(), collector);
    return collector;
  }

  private static void findLexerActionsRecursive(
      ATNState state, IntHashSet alreadyPassed, IntHashSet collector) {
    for (Transition transition : state.getTransitions()) {
      // This must go before it.isEpsilon because ActionTransitions are epsilon
      if (transition.getSerializationType() == Transition.ACTION) {
        ActionTransition action = (ActionTransition) transition;
        collector.add(action.actionIndex);
        IntHashSet newPassed = new IntHashSet();
        newPassed.add(transition.target.stateNumber);
        findLexerActionsRecursive(transition.target, newPassed, collector);
      } else if (transition.isEpsilon()) {
        if (!alreadyPassed.contains(transition.target.stateNumber)) {
          IntHashSet newPassed = new IntHashSet();
          newPassed.addAll(alreadyPassed);
          newPassed.add(transition.target.stateNumber);
          findLexerActionsRecursive(transition.target, newPassed, collector);
        }
      } else {
        findLexerActionsRecursive(transition.target, alreadyPassed, collector);
      }
    }
  }

  private static List<Suggestion> groupSuggestions(Collection<Suggestion> suggestions) {
    Int2ObjectHashMap<Suggestion> register = new Int2ObjectHashMap<>();
    List<Suggestion> grouped = new ArrayList<>();
    for (Suggestion s : suggestions) {
      Suggestion g = register.put(s.id, s);
      if (g != null) {
        g.addCtx(s);
      } else {
        grouped.add(s);
      }
    }
    return grouped;
  }

  @Value.Immutable
  interface ParseState {
    int ruleIndex();

    ATNState followState();

    int precedence();

    static ParseState parseState(int ruleIndex, ATNState state, int precedence) {
      return ImmutableParseState.of(ruleIndex, state, precedence);
    }
  }

  @Value.Immutable
  interface ProcessStackElement {
    int tokenStreamIndex();

    ATNState state();

    IntHashSet alreadyPassed();

    List<ParseState> parserStack();

    @Nullable
    RecoveryData recoveryData();

    static ProcessStackElement processStackElement(
        int tokenStreamIndex,
        ATNState state,
        IntHashSet alreadyPassed,
        List<ParseState> parserStack,
        @Nullable RecoveryData recoveryData) {
      return ImmutableProcessStackElement.of(
          tokenStreamIndex, state, alreadyPassed, parserStack, recoveryData);
    }
  }

  @Value.Immutable
  interface RecoveryData {
    int suggestions();

    List<RecoveryRule> recoveryRules();
  }
}
