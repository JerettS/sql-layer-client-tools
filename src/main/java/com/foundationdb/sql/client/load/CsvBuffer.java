/**
 * Copyright (C) 2009-2013 FoundationDB, LLC
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

package com.foundationdb.sql.client.load;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class CsvBuffer implements StatementBuffer<List<String>>
{
    private static final int UNSET = -1;
    private static final char DELIM = ',';
    private static final char QUOTE = '\"';
    private static final char NEWLINE = '\n';
    private static final char CARRIAGE_RETURN = '\r';

    private List<String> values;
    private int endIndex;
    private int currentIndex;
    private StringBuilder rowBuffer;
    private StringBuilder currentField = new StringBuilder();
    private State state;

    private enum State { ROW_START, FIELD_START, IN_FIELD, IN_QUOTE, AFTER_QUOTE };

    public CsvBuffer() {
        this.rowBuffer = new StringBuilder();
        reset();
    }

    private void reset() {
        this.endIndex = UNSET;
        this.values = new ArrayList();
        this.currentIndex = 0;
        this.state = State.ROW_START;
        this.currentField = new StringBuilder();
        rowBuffer.setLength(0);
    }

    @Override
    public void append(char c) {
        rowBuffer.append(c);
    }

    @Override
    public List<String> nextStatement() {
        if (endIndex == UNSET) {
            throw new IllegalArgumentException("No Row Present");
        }
        List<String> values = this.values;
        reset();
        return values;
    }

    @Override
    public boolean hasStatement(boolean endOfFile) throws IOException, LineReader.ParseException {
        if (endOfFile && !(rowBuffer.length() == 0)) {
            append(NEWLINE); // replace the \n
        }
        return hasStatement();
    }

    private boolean hasStatement() throws IOException, LineReader.ParseException {
        while (currentIndex < rowBuffer.length()) {
            char ch = rowBuffer.charAt(currentIndex++);
            switch (state) {
            case ROW_START:
                handleRowStart(ch);
                break;
            case FIELD_START:
                handleFieldStart(ch);
                break;
            case IN_FIELD:
                handleInField(ch);
                break;
            case IN_QUOTE:
                handleInQuote(ch);
                break;
            case AFTER_QUOTE:
                handleAfterQuote(ch);
                break;
            }
        }
        if (state == State.FIELD_START || state == State.AFTER_QUOTE || state == State.IN_FIELD) {
            endIndex = currentIndex;
            values.add(currentField.toString());
            currentField.setLength(0);
            state = State.ROW_START;
        }
        return endIndex != UNSET;
    }

    private void handleRowStart(char b) {
        if ((b == CARRIAGE_RETURN) || (b == NEWLINE)) {
        }
        else if (b == DELIM) {
            values.add("");
            state = State.FIELD_START;
        }
        else if (b == QUOTE) {
            state = State.IN_QUOTE;
        }
        else {
            currentField.append(b);
            state = State.IN_FIELD;
        }
    }

    private void handleFieldStart(char b) {
        if ((b == CARRIAGE_RETURN) || (b == NEWLINE)) {
            values.add(currentField.toString());
            endIndex = currentIndex;
            state = State.ROW_START;
        }
        else if (b == DELIM) {
            values.add(currentField.toString());
            currentField.setLength(0);
        }
        else if (b == QUOTE) {
            state = State.IN_QUOTE;
        }
        else {
            currentField.append(b);
            state = State.IN_FIELD;
        }
    }

    private void handleInField(char b) throws LineReader.ParseException {
        if ((b == CARRIAGE_RETURN) || (b == NEWLINE)) {
            values.add(currentField.toString());
            currentField.setLength(0);
            endIndex = currentIndex;
            state = State.ROW_START;
        }
        else if (b == DELIM) {
            values.add(currentField.toString());
            currentField.setLength(0);
            state = State.FIELD_START;
        }
        else if (b == QUOTE) {
            throw new LineReader.ParseException(
                    "CSV File contains QUOTE in the middle of a field and cannot be fast loaded : " + rowBuffer);
        }
        else {
            currentField.append(b);
        }
    }

    private void handleInQuote(char b) {
        if (b == QUOTE) {
            state = State.AFTER_QUOTE;
        }
        else {
            currentField.append(b);
        }
    }

    private void handleAfterQuote(char b) throws LineReader.ParseException {
        if ((b == CARRIAGE_RETURN) || (b == NEWLINE)) {
            values.add(currentField.toString());
            currentField.setLength(0);
            endIndex = currentIndex;
            state = State.ROW_START;
        }
        else if (b == DELIM) {
            values.add(currentField.toString());
            currentField.setLength(0);
            state = State.FIELD_START;
        }
        else if (b == QUOTE) {
            currentField.append(b);
            state = State.IN_QUOTE;
        }
        else {
            throw new LineReader.ParseException(
                    "CSV File contains junk after quoted field and cannot be fast loaded : " + rowBuffer);
        }
    }

}
