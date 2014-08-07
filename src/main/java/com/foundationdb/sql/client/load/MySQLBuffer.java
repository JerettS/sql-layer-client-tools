/**
 * Copyright (C) 2012-2013 FoundationDB, LLC
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License, version 3,
 * as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see http://www.gnu.org/licenses.
 */

package com.foundationdb.sql.client.load;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.nio.charset.UnsupportedCharsetException;
import java.util.ArrayList;
import java.util.List;

/**
 * Input accumulator and simple parser for two query types, backslash and semi-colon delimited.
 *
 * <p>Backslash queries consume up to a newline, or end of buffer, and are found with optional whitespace preceding a backslash.</p>
 * <p>Semi-colon delimited splits on un-quoted (', " or `) semi-colons with any trailing remaining in buffer.</p>
 */
public class MySQLBuffer
{
    private static final int UNSET = -1;
    private final int delim, quote, escape, nl, cr, statementEnd;
    private final String encoding;

    private List<String> values;
    private int startIndex, endIndex, currentIndex;
    private StringBuilder rowBuffer;
    private StringBuilder currentField = new StringBuilder();
    private StringBuilder preparedStatement = new StringBuilder();
    private String currentTable;
    private char quoteChar;
    private State state;
    private boolean firstRow, firstField, escapedChar, swallowWhitespace;

    private enum State { STATEMENT_START, LINE_COMMENT_START, SINGLE_LINE_COMMENT,
                         AFTER_FORWARD_SLASH, DELIMITED_COMMENT, FINISHING_DELIMITED_COMMENT,
                         STATEMENT_VERB, IGNORED_STATEMENT, IGNORED_STATEMENT_QUOTE,
                         INSERT, INSERT_TABLE_NAME, INSERT_TABLE_QUOTED, INSERT_VALUES_KEYWORD,
                         ROW_START, FIELD_START };

    public MySQLBuffer(String encoding) {
        this.encoding = encoding;
        this.statementEnd = getSingleByte(";");
        this.delim = getSingleByte(",");
        this.quote = getSingleByte("\"");
        this.escape = quote;
        this.nl = getSingleByte("\n");
        this.cr = getSingleByte("\r");
        this.rowBuffer = new StringBuilder();
        reset();
    }

    private byte[] getBytes(String str) {
        try {
            return str.getBytes(encoding);
        }
        catch (UnsupportedEncodingException ex) {
            UnsupportedCharsetException nex = new UnsupportedCharsetException(encoding);
            nex.initCause(ex);
            throw nex;
        }
    }

    private int getSingleByte(String str) {
        byte[] bytes = getBytes(str);
        if (bytes.length != 1)
            throw new IllegalArgumentException("Must encode as a single byte.");
        return bytes[0] & 0xFF;
    }

    public void reset() {
        this.startIndex = 0;
        this.endIndex = UNSET;
        this.values = new ArrayList();
        this.currentIndex = 0;
        this.state = State.STATEMENT_START;
        this.currentField = new StringBuilder();
        rowBuffer.setLength(0);
        firstRow = true;
        firstField = true;
        swallowWhitespace = true;
        preparedStatement.setLength(0);
    }

    public boolean isEmpty() {
        return rowBuffer.length() == 0;
    }

    public void append(String string) {
        rowBuffer.append(string);
    }

    public void append(char c) {
        rowBuffer.append(c);
    }

    public Query nextQuery() {
        return new Query("",new String[0]);
        // if (endIndex == UNSET) {
        //     throw new IllegalArgumentException("No Row Present");
        // }
        // List<String> values = this.values;
        // reset();
        // return values;
    }

    public boolean hasQuery() throws IOException {
        while (currentIndex < rowBuffer.length()) {
            // DO NOT increment currentIndex within the switch
            // because we may be on the last character in the buffer
            // and if you do that whatever state you have will be lost
            char c = rowBuffer.charAt(currentIndex++);
            if (swallowWhitespace) {
                if (c == ' ') {
                    continue;
                } else {
                    swallowWhitespace = false;
                }
            }
            switch (state) {
            case STATEMENT_START:
                if ((c == cr) || (c == nl) || (c == ';')) {
                    continue;
                }
                else if (c == '-') {
                    state = State.LINE_COMMENT_START;
                    continue;
                } else if (c == '/') {
                    state = State.AFTER_FORWARD_SLASH;
                    continue;
                } else if (Character.isLetter(c)) {
                    clearCurrentField();
                    currentField.append(c);
                    state = State.STATEMENT_VERB;
                    continue;
                } else {
                    throw new RuntimeException("TODO " + state + ": " + c);
                }
            case LINE_COMMENT_START:
                if (c == '-') {
                    state = State.SINGLE_LINE_COMMENT;
                    continue;
                } else {
                    throw new UnexpectedTokenException('-', c);
                }
            case SINGLE_LINE_COMMENT:
                if (c == nl) {
                    state = State.STATEMENT_START;
                    continue;
                } // else ignore
                break;
            case AFTER_FORWARD_SLASH:
                if (c == '*') {
                    state = State.DELIMITED_COMMENT;
                    continue;
                } else {
                    throw new UnexpectedTokenException('*',c);
                }
            case DELIMITED_COMMENT:
                if (c == '*') {
                    state = State.FINISHING_DELIMITED_COMMENT;
                    continue;
                } // else ignore
                break;
            case FINISHING_DELIMITED_COMMENT:
                if (c == '/') {
                    state = State.STATEMENT_START;
                    continue;
                } else {
                    // back to comment
                    state = State.DELIMITED_COMMENT;
                    continue;
                }
            case STATEMENT_VERB:
                if (readKeyword(c)) {
                    String s = currentField.toString();
                    if (s.equalsIgnoreCase("lock") || s.equalsIgnoreCase("unlock")) {
                        state = State.IGNORED_STATEMENT;
                        continue;
                    } else if (s.equalsIgnoreCase("insert")) {
                        swallowWhitespace = true;
                        state = State.INSERT;
                        clearCurrentField();
                        continue;
                    } else {
                        throw new UnexpectedKeyword("INSERT", currentField.toString());
                    }
                }
                break;
            case IGNORED_STATEMENT:
                if (c == ';') {
                    clearCurrentField();
                    swallowWhitespace = true;
                    state = State.STATEMENT_START;
                    continue;
                } else if (isQuote(c)) {
                    quoteChar = c;
                    state = State.IGNORED_STATEMENT_QUOTE;
                    continue;
                } else {
                    // ignored character
                    continue;
                }
            case IGNORED_STATEMENT_QUOTE:
                if (escapedChar) {
                    escapedChar = false;
                    continue;
                } else if (c == quoteChar) {
                    state = State.IGNORED_STATEMENT;
                    continue;
                } else if (c == '\\') {
                    escapedChar = true;
                    continue;
                } else {
                    // ignored character
                    continue;
                }
            case INSERT:
                if (readKeyword(c)) {
                    String s = currentField.toString();
                    if (s.equalsIgnoreCase("into")) {
                        swallowWhitespace = true;
                        state = State.INSERT_TABLE_NAME;
                        preparedStatement.append("INSERT INTO ");
                        continue;
                    } else {
                        throw new RuntimeException("TODO");
                    }
                }
                break;
            case INSERT_TABLE_NAME:
                if (isQuote(c)) {
                    quoteChar = c;
                    state = State.INSERT_TABLE_QUOTED;
                    continue;
                } else if (isIdentifierCharacter(c)) {
                    currentField.append(c);
                    continue;
                } else if (c == ' ') {
                    setTableName();
                    swallowWhitespace = true;
                    state = State.INSERT_VALUES_KEYWORD;
                    continue;
                } else {
                    throw new UnexpectedTokenException("a valid identifier character", c);
                }
            case INSERT_TABLE_QUOTED:
                if (escapedChar) {
                    currentField.append(c);
                    escapedChar = false;
                    continue;
                } else if (c == quoteChar) {
                    setTableName();
                    swallowWhitespace = true;
                    state = State.INSERT_VALUES_KEYWORD;
                    continue;
                } else if (c == '\\') {
                    escapedChar = true;
                    continue;
                } else {
                    currentField.append(c);
                    continue;
                }
            case INSERT_VALUES_KEYWORD:
                if (readKeyword(c)) {
                    String s = currentField.toString();
                    if (s.equalsIgnoreCase("values")) {
                        state = State.ROW_START;
                        preparedStatement.append("VALUES ");
                        continue;
                    } else {
                        System.out.println(rowBuffer.substring(0,currentIndex));
                        throw new UnexpectedKeyword("VALUES", s);
                    }
                }
                break;
            case ROW_START:
                if (c == '(') {
                    addRow();
                    state = State.FIELD_START;
                    continue;
                } else {
                    throw new UnexpectedTokenException('(', c);
                }
            default:
                throw new RuntimeException("TODO " + state + ": " + c);
            }
        }
        return endIndex != UNSET;
    }

    private boolean isIdentifierCharacter(char c) {
        return Character.isLetterOrDigit(c) || c == '_' || c == '$' || (c >= '\u0080' && c <= '\uFFFF');
    }

    private boolean isQuote(char c) {
        return c == '`' || c == '\'' || c == '"';
    }

    private boolean readKeyword(char c) throws UnexpectedTokenException {
        if (Character.isLetter(c)) {
            currentField.append(c);
            return false;
        } else if (Character.isSpace(c)) {
            return true;
        } else {
            throw new UnexpectedTokenException("a letter", c);
        }
    }

    private static String escapeIdentifier(String identifier) {
        return identifier.replaceAll("\"","\"\"");
    }

    private void clearCurrentField() {
        currentField.setLength(0);
    }

    private void setTableName() {
        preparedStatement.append('"');
        preparedStatement.append(escapeIdentifier(currentField.toString()));
        preparedStatement.append('"');
        preparedStatement.append(' ');
        clearCurrentField();
    }

    private void addRow() {
        firstField = true;
        if (firstRow) {
            preparedStatement.append("(");
            firstRow = false;
        } else {
            preparedStatement.append("), (");
            throw new RuntimeException("TODO combined rows");
        }
    }

    private void addField(String s) {
        if (firstField) {
            preparedStatement.append("?");
            firstField = false;
        } else {
            preparedStatement.append(", ?");
            throw new RuntimeException("TODO multiple columns");
        }
    }


    public static class Query {
        private String preparedStatement;
        private String[] values;

        public Query(String preparedStatement, String[] values) {
            this.preparedStatement = preparedStatement;
            this.values = values;
        }

        public String getPreparedStatement() {
            return preparedStatement;
        }

        public String[] getValues() {
            return values;
        }
    }

    // TODO probably this shouldn't be an IOException
    public static class UnexpectedTokenException extends IOException {
        private String expected;
        private char actual;

        public UnexpectedTokenException(char expected, char actual) {
            super("Error parsing mysql: expected to get '" + expected + "' but got the token '" + actual + "'");
            this.expected = "'" + expected + "'";
            this.actual = actual;
        }

        public UnexpectedTokenException(String expected, char actual) {
            super("Error parsing mysql: expected to get " + expected + " but got the token '" + actual + "'");
            this.expected = expected;
            this.actual = actual;
        }

        public String getExpected() {
            return expected;
        }

        public char getActual() {
            return actual;
        }
    }

    // TODO probably this shouldn't be an IOException
    public static class UnexpectedKeyword extends IOException {
        private String actual;
        private String expected;

        public UnexpectedKeyword(String expected, String actual) {
            super("Error parsing mysql: Expected keyword: " + expected + " but got the word: " + actual);
            this.actual = actual;
            this.expected = expected;
        }

        public String getActual() {
            return actual;
        }

        public String getExpected() {
            return expected;
        }
    }

}
