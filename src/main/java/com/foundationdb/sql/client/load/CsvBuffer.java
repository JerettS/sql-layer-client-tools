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
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.nio.charset.UnsupportedCharsetException;
import java.util.ArrayList;
import java.util.List;

public class CsvBuffer
{
    private static final int UNSET = -1;
    private final int delim, quote, escape, nl, cr;
    private final String encoding;

    private List<String> values;
    private int endIndex;
    private int currentIndex;
    private StringBuilder rowBuffer;
    private StringBuilder currentField = new StringBuilder();
    private State state;

    private enum State { ROW_START, FIELD_START, IN_FIELD, IN_QUOTE, AFTER_QUOTE };

    public CsvBuffer(String encoding) {
        this.encoding = encoding;
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
        this.endIndex = UNSET;
        this.values = new ArrayList();
        this.currentIndex = 0;
        this.state = State.ROW_START;
        this.currentField = new StringBuilder();
        rowBuffer.setLength(0);
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

    public List<String> nextRow() {
        if (endIndex == UNSET) {
            throw new IllegalArgumentException("No Row Present");
        }
        List<String> values = this.values;
        reset();
        return values;
    }

    public boolean hasRow() throws IOException {
        while (currentIndex < rowBuffer.length()) {
            char b = rowBuffer.charAt(currentIndex++);
            switch (state) {
            case ROW_START:
                handleRowStart(b);
                break;
            case FIELD_START:
                handleFieldStart(b);
                break;
            case IN_FIELD:
                handleInField(b);
                break;
            case IN_QUOTE:
                handleInQuote(b);
                break;
            case AFTER_QUOTE:
                handleAfterQuote(b);
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
        if ((b == cr) || (b == nl)) {
        }
        else if (b == delim) {
            values.add("");
            state = State.FIELD_START;
        }
        else if (b == quote) {
            state = State.IN_QUOTE;
        }
        else {
            currentField.append(b);
            state = State.IN_FIELD;
        }
    }

    private void handleFieldStart(char b) {
        if ((b == cr) || (b == nl)) {
            values.add(currentField.toString());
            endIndex = currentIndex;
            state = State.ROW_START;
        }
        else if (b == delim) {
            values.add(currentField.toString());
            currentField.setLength(0);
        }
        else if (b == quote) {
            state = State.IN_QUOTE;
        }
        else {
            currentField.append(b);
            state = State.IN_FIELD;
        }
    }

    private void handleInField(char b) {
        if ((b == cr) || (b == nl)) {
            values.add(currentField.toString());
            currentField.setLength(0);
            endIndex = currentIndex;
            state = State.ROW_START;
        }
        else if (b == delim) {
            values.add(currentField.toString());
            currentField.setLength(0);
            state = State.FIELD_START;
        }
        else if (b == quote) {
            throw new UnsupportedOperationException(
                    "CSV File contains QUOTE in the middle of a field and cannot be fast loaded : " + rowBuffer);
        }
        else {
            currentField.append(b);
        }
    }

    private void handleInQuote(char b) {
        if (b == quote) {
            state = State.AFTER_QUOTE;
        }
        else {
            currentField.append(b);
        }
    }

    private void handleAfterQuote(char b) {
        if ((b == cr) || (b == nl)) {
            values.add(currentField.toString());
            currentField.setLength(0);
            endIndex = currentIndex;
            state = State.ROW_START;
        }
        else if (b == delim) {
            values.add(currentField.toString());
            currentField.setLength(0);
            state = State.FIELD_START;
        }
        else if (b == quote) {
            currentField.append(b);
            state = State.IN_QUOTE;
        }
        else {
            throw new UnsupportedOperationException(
                    "CSV File contains junk after quoted field and cannot be fast loaded : " + rowBuffer);
        }
    }

}
