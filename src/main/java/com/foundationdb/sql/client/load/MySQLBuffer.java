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
import java.util.Arrays;
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
    private int currentIndex;
    private StringBuilder rowBuffer;
    private StringBuilder currentField = new StringBuilder();
    private StringBuilder preparedStatement = new StringBuilder();
    private Query query;
    private char quoteChar;
    private State state;
    private boolean firstRow, firstField, escapedChar, swallowWhitespace;
    private static final String[] emptyStringForToArray = new String[0];

    private enum State { STATEMENT_START, LINE_COMMENT_START, SINGLE_LINE_COMMENT,
                         AFTER_FORWARD_SLASH, DELIMITED_COMMENT, FINISHING_DELIMITED_COMMENT,
                         STATEMENT_VERB, IGNORED_STATEMENT, IGNORED_STATEMENT_QUOTE,
                         INSERT, INSERT_TABLE_NAME, INSERT_TABLE_QUOTED, INSERT_VALUES_KEYWORD,
                         ROW_START, AFTER_ROW, FIELD_START, FIELD, QUOTED_FIELD, AFTER_QUOTED_FIELD };

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
        this.values = new ArrayList();
        this.currentIndex = 0;
        rowBuffer.setLength(0);
        reset(UNSET);
    }

    public void reset(int endIndex) {
        query = new Query(preparedStatement.toString(), values.toArray(emptyStringForToArray));
        if (endIndex >= 0) {
            rowBuffer.delete(0,endIndex);
            currentIndex -= endIndex;
        }
        preparedStatement.setLength(0);
        values.clear();
        firstRow = true;
        firstField = true;
        swallowWhitespace = true;
        this.state = State.STATEMENT_START;
        this.currentField.setLength(0);
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
        return query;
    }

    public boolean hasQuery(boolean endOfFile) throws IOException, ParseException {
        if (hasQuery()) {
            return true;
        } else if (endOfFile && state != State.STATEMENT_START) {
            throw new UnexpectedEndOfFileException();
        }
        return false;
    }

    public boolean hasQuery() throws IOException, ParseException {
        while (currentIndex < rowBuffer.length()) {
            // DO NOT increment currentIndex within the switch
            // because we may be on the last character in the buffer
            // and if you do that whatever state you have will be lost
            char c = rowBuffer.charAt(currentIndex++);
            if (swallowWhitespace) {
                if (Character.isWhitespace(c)) {
                    continue;
                } else {
                    swallowWhitespace = false;
                }
            }
            switch (state) {
            case STATEMENT_START:
                handleStatementStart(c);
                break;
            case LINE_COMMENT_START:
                if (c == '-') {
                    state = State.SINGLE_LINE_COMMENT;
                } else {
                    throw new UnexpectedTokenException('-', c);
                }
                break;
            case SINGLE_LINE_COMMENT:
                if (c == nl) {
                    state = State.STATEMENT_START;
                } // else ignore
                break;
            case AFTER_FORWARD_SLASH:
                if (c == '*') {
                    state = State.DELIMITED_COMMENT;
                } else {
                    throw new UnexpectedTokenException('*',c);
                }
                break;
            case DELIMITED_COMMENT:
                if (c == '*') {
                    state = State.FINISHING_DELIMITED_COMMENT;
                } // else ignore
                break;
            case FINISHING_DELIMITED_COMMENT:
                handleFinishingDelimitedComment(c);
                break;
            case STATEMENT_VERB:
                handleStatementVerb(c);
                break;
            case IGNORED_STATEMENT:
                handleIgnoredStatement(c);
                break;
            case IGNORED_STATEMENT_QUOTE:
                handleIgnoredStatementQuote(c);
                break;
            case INSERT:
                handleInsert(c);
                break;
            case INSERT_TABLE_NAME:
                handleInsertTableName(c);
                break;
            case INSERT_TABLE_QUOTED:
                handleInsertTableQuoted(c);
                break;
            case INSERT_VALUES_KEYWORD:
                handleValuesKeyword(c);
                break;
            case ROW_START:
                handleRowStart(c);
                break;
            case AFTER_ROW:
                if (handleAfterRow(c)) {
                    return true;
                }
                break;
            case FIELD_START:
                handleFieldStart(c);
                break;
            case FIELD:
                handleField(c);
                break;
            case QUOTED_FIELD:
                handleQuotedField(c);
                break;
            case AFTER_QUOTED_FIELD:
                handleAfterQuotedField(c);
                break;
            }
        }
        return false;
    }

    private void handleStatementStart(char c) throws UnexpectedTokenException {
        if ((c == cr) || (c == nl) || (c == ';')) {
            return;
        }
        else if (c == '-') {
            state = State.LINE_COMMENT_START;
        } else if (c == '/') {
            state = State.AFTER_FORWARD_SLASH;
        } else if (Character.isLetter(c)) {
            clearCurrentField();
            currentField.append(c);
            state = State.STATEMENT_VERB;
        } else {
            throw new UnexpectedTokenException("a statement start", 'c');
        }
    }

    private void handleStatementVerb(char c) throws UnexpectedTokenException, UnexpectedKeyword {
        if (readKeyword(c)) {
            String s = currentField.toString();
            if (s.equalsIgnoreCase("lock") || s.equalsIgnoreCase("unlock")) {
                state = State.IGNORED_STATEMENT;
            } else if (s.equalsIgnoreCase("insert")) {
                swallowWhitespace = true;
                state = State.INSERT;
                clearCurrentField();
            } else {
                throw new UnexpectedKeyword("INSERT", currentField.toString());
            }
        }
    }

    private void handleFinishingDelimitedComment(char c) {
        if (c == '/') {
            state = State.STATEMENT_START;
        } else {
            // back to comment
            state = State.DELIMITED_COMMENT;
        }
    }

    private void handleIgnoredStatement(char c) {
        if (c == ';') {
            clearCurrentField();
            swallowWhitespace = true;
            state = State.STATEMENT_START;
        } else if (isQuote(c)) {
            quoteChar = c;
            state = State.IGNORED_STATEMENT_QUOTE;
        } else {
            // ignored character
        }
    }

    private void handleIgnoredStatementQuote(char c) {
        if (escapedChar) {
            escapedChar = false;
        } else if (c == quoteChar) {
            state = State.IGNORED_STATEMENT;
        } else if (c == '\\') {
            escapedChar = true;
        } else {
            // ignored character
        }
    }

    private void handleInsert(char c) throws UnexpectedTokenException, UnexpectedKeyword {
        if (readKeyword(c)) {
            String s = currentField.toString();
            if (s.equalsIgnoreCase("into")) {
                swallowWhitespace = true;
                state = State.INSERT_TABLE_NAME;
                preparedStatement.append("INSERT INTO ");
                clearCurrentField();
            } else {
                throw new UnexpectedKeyword("INTO", currentField.toString());
            }
        }
    }

    private void handleInsertTableName(char c) throws UnexpectedTokenException {
        if (isQuote(c)) {
            quoteChar = c;
            state = State.INSERT_TABLE_QUOTED;
        } else if (isIdentifierCharacter(c)) {
            currentField.append(c);
        } else if (c == ' ') {
            setTableName();
            swallowWhitespace = true;
            state = State.INSERT_VALUES_KEYWORD;
        } else {
            throw new UnexpectedTokenException("a valid identifier character", c);
        }
    }

    private void handleInsertTableQuoted(char c) {
        if (escapedChar) {
            currentField.append(c);
            escapedChar = false;
        } else if (c == quoteChar) {
            setTableName();
            swallowWhitespace = true;
            state = State.INSERT_VALUES_KEYWORD;
        } else if (c == '\\') {
            escapedChar = true;
        } else {
            currentField.append(c);
        }
    }

    private void handleValuesKeyword(char c) throws UnexpectedTokenException, UnexpectedKeyword {
        if (readKeyword(c)) {
            String s = currentField.toString();
            if (s.equalsIgnoreCase("values")) {
                state = State.ROW_START;
                preparedStatement.append("VALUES ");
            } else {
                throw new UnexpectedKeyword("VALUES", s);
            }
        }
    }

    private void handleRowStart(char c) throws UnexpectedTokenException {
        if (c == '(') {
            firstField = true;
            if (firstRow) {
                preparedStatement.append("(");
                firstRow = false;
            } else {
                preparedStatement.append(", (");
            }
            state = State.FIELD_START;
        } else {
            throw new UnexpectedTokenException('(', c);
        }
    }

    private void handleFieldStart(char c) {
        clearCurrentField();
        if (isQuote(c)) {
            quoteChar = c;
            clearCurrentField();
            state = State.QUOTED_FIELD;
        } else {
            currentField.append(c);
            state = State.FIELD;
        }
    }

    private void handleField(char c) throws UnexpectedTokenException {
        if (c == ')') {
            addField();
            endRow();
            swallowWhitespace = true;
            state = State.AFTER_ROW;
        } else if (c == ',') {
            addField();
            swallowWhitespace = true;
            state = State.FIELD_START;
        } else if (isQuote(c)) {
            throw new UnexpectedTokenException("a literal character", '\'');
        } else {
            currentField.append(c);
        }
    }

    private void handleQuotedField(char c) {
        if (escapedChar) {
            escapedChar = false;
            switch (c) {
            case '0':
                currentField.append('\000');
                break;
            case 'b':
                currentField.append('\b');
                break;
            case 'n':
                currentField.append('\n');
                break;
            case 'r':
                currentField.append('\r');
                break;
            case 't':
                currentField.append('\t');
                break;
            case 'Z':
                // The ASCII 26 character can be encoded as “\Z” to enable you to work
                //  around the problem that ASCII 26 stands for END-OF-FILE on Windows.
                //  ASCII 26 within a file causes problems if you try to use mysql db_name < file_name.
                currentField.append('\u001A');
                break;
            default:
                currentField.append(c);
                break;
            }
        } else if (c == quoteChar) {
            swallowWhitespace = true;
            state = State.AFTER_QUOTED_FIELD;
        } else if (c == '\\') {
            escapedChar = true;
        } else {
            currentField.append(c);
        }
    }

    private void handleAfterQuotedField(char c) throws UnexpectedTokenException {
        if (c == quoteChar) {
            currentField.append(c);
            state = State.QUOTED_FIELD;
        } else if (c == ',') {
            addField();
            swallowWhitespace = true;
            state = State.FIELD_START;
        } else if (c == ')') {
            addField();
            swallowWhitespace = true;
            endRow();
            state = State.AFTER_ROW;
        } else {
            throw new UnexpectedTokenException("',' or ')'", c);
        }
    }

    private boolean handleAfterRow(char c) throws UnexpectedTokenException {
        if (c == ',') {
            swallowWhitespace = true;
            state = State.ROW_START;
        } else if (c == ';') {
            if (values.size() == 0) {
                throw new UnexpectedTokenException("a row", ';');
            } else {
                state = State.STATEMENT_START;
                reset(currentIndex);
                return true;
            }
        } else {
            throw new UnexpectedTokenException("a ',' or ';'", c);
        }
        return false;
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

    private void endRow() {
        preparedStatement.append(")");
    }

    private void addField() {
        if (firstField) {
            preparedStatement.append("?");
            firstField = false;
        } else {
            preparedStatement.append(", ?");
        }
        values.add(currentField.toString());
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

        @Override
        public boolean equals(Object other) {
            if (other instanceof Query) {
                Query otherQuery = (Query)other;
                return equalStrings(preparedStatement,otherQuery.preparedStatement) &&
                    equalArrays(values, otherQuery.values);
            } else {
                return false;
            }
        }

        @Override
        public String toString() {
            return preparedStatement + "; " + Arrays.toString(values);
        }

        private boolean equalStrings(String a, String b) {
            if (a == null) {
                return b == null;
            } else {
                return a.equals(b);
            }
        }

        private boolean equalArrays(String[] as, String[] bs) {
            if (as.length != bs.length) {
                return false;
            } else {
                for (int i=0; i<as.length; i++) {
                    if (!equalStrings(as[i],bs[i])) {
                        return false;
                    }
                }
                return true;
            }
        }
    }

    public static class ParseException extends LineReader.ParseException {
        public ParseException(String message) {
            super("Error parsing mysql: " + message);
        }
    }

    public static class UnexpectedTokenException extends ParseException {
        private String expected;
        private char actual;

        public UnexpectedTokenException(char expected, char actual) {
            super("expected to get '" + expected + "' but got the token '" + actual + "'");
            this.expected = "'" + expected + "'";
            this.actual = actual;
        }

        public UnexpectedTokenException(String expected, char actual) {
            super("expected to get " + expected + " but got the token '" + actual + "'");
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

    public static class UnexpectedKeyword extends ParseException {
        private String actual;
        private String expected;

        public UnexpectedKeyword(String expected, String actual) {
            super("Expected keyword: " + expected + " but got the word: " + actual);
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

    public static class UnexpectedEndOfFileException extends ParseException {
        public UnexpectedEndOfFileException() {
            super("End of file mid statement");
        }
    }

}
