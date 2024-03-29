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
grammar NessieCli;

script
    :
      ( statement SEMICOLON )* (statement SEMICOLON?)?
      EOF
    ;

statement
    : createReferenceStatement
    | dropReferenceStatement
    | assignReferenceStatement
    | useReferenceStatement
    | listReferencesStatement
    | showReferenceStatement
    | mergeBranchStatement
    | showLogStatement
    ;

createReferenceStatement
    : CREATE refType=referenceType (conditional=ifNotExists)? reference=referenceName (FROM fromRef=referenceSpec)?
    ;

dropReferenceStatement
    : DROP refType=referenceType (conditional=ifExists)? reference=existingReference
    ;

assignReferenceStatement
    : ASSIGN refType=referenceType (reference=existingReference)? (TO toRef=existingReference (AT toHash=referenceSpec)?)?
    ;

useReferenceStatement
    : USE REFERENCE reference=existingReference (AT tsOrHash=timestampOrHash)?
    ;

listReferencesStatement
    : LIST REFERENCES
    ;

showReferenceStatement
    : SHOW REFERENCE
    ;

mergeBranchStatement
    : MERGE (refType=referenceType)? (reference=existingReference)? (INTO toRef=existingReference)?
    ;

showLogStatement
    : SHOW LOG (reference=existingReference)?
    ;

referenceSpec
    // TODO define - can be one of:
    //   - a reference name
    //   - a reference name with a hash
    //   - a hash
    //   - a reference name with timestamp
    //   - a timestamp (assuming the "current" named reference)
    : identifier
    ;

timestampOrHash
    // TODO define
    : identifier
    ;

referenceName
    : identifier
    ;

existingReference
    : identifier
    ;

identifier
    : IDENTIFIER              #unquotedIdentifier
    | quotedIdentifier        #quotedIdentifierAlternative
    ;

quotedIdentifier
    : BACKQUOTED_IDENTIFIER
    ;

referenceType
    : type = (BRANCH | TAG)
    ;

ifNotExists
    : IF NOT EXISTS
    ;

ifExists
    : IF EXISTS
    ;

ASSIGN : 'ASSIGN';
AT : 'AT';
BRANCH : 'BRANCH';
CREATE : 'CREATE';
DROP : 'DROP';
EXISTS : 'EXISTS';
FROM : 'FROM';
IF : 'IF';
INTO : 'INTO';
LIST : 'LIST';
LOG : 'LOG';
MERGE : 'MERGE';
NOT : 'NOT';
REFERENCE : 'REFERENCE';
REFERENCES : 'REFERENCES';
SHOW : 'SHOW';
TAG : 'TAG';
TO : 'TO';
USE : 'USE';

SEMICOLON : ';';

STRING
    : '\'' ( ~('\''|'\\') | ('\\' .) )* '\''
    | '"' ( ~('"'|'\\') | ('\\' .) )* '"'
    ;

INTEGER_VALUE
    : DIGIT+
    ;

IDENTIFIER
    : (LETTER | DIGIT | '_')+
    ;

BACKQUOTED_IDENTIFIER
    : '`' ( ~'`' | '``' )* '`'
    ;

fragment DECIMAL_DIGITS
    : DIGIT+ '.' DIGIT*
    | '.' DIGIT+
    ;

fragment EXPONENT
    : 'E' [+-]? DIGIT+
    ;

fragment DIGIT
    : [0-9]
    ;

fragment LETTER
    : [A-Za-z]
    ;

SIMPLE_COMMENT
    : '--' ('\\\n' | ~[\r\n])* '\r'? '\n'? -> skip
    ;

BRACKETED_COMMENT
    : '/*' (BRACKETED_COMMENT|.)*? '*/' -> skip
    ;

WS
    // Use channel(HIDDEN) to let whitespaces appear in error messages
    : [ \r\n\t\p{White_Space}]+ -> channel(HIDDEN)
    ;

// Catch-all for anything we can't recognize.
// We use this to be able to ignore and recover all the text
// when splitting statements with DelimiterLexer
UNRECOGNIZED
    : .
    ;
