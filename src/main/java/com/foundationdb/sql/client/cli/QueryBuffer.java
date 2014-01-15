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

package com.foundationdb.sql.client.cli;

/**
 * Input accumulator and simple parser for two query types, backslash and semi-colon delimited.
 *
 * <p>Backslash queries consume the entire buffer and are found with optional whitespace preceding a backslash.</p>
 * <p>Semi-colon delimited splits on un-quoted (', " or `) semi-colons with any trailing remaining in buffer.</p>
 */
public class QueryBuffer
{
    private static final int UNSET = -1;

    private final StringBuilder buffer = new StringBuilder();
    private int curIndex;
    private int startIndex;
    private int endIndex;
    private int quoteChar;
    private boolean isOnlySpace;
    private boolean isBackslash;

    public QueryBuffer() {
        reset();
    }

    public void append(char c) {
        buffer.append(c);
    }

    public void append(CharSequence cs) {
        buffer.append(cs);
    }

    /** As append() with automatic '--' stripping. */
    public void appendLine(CharSequence cs) {
        append(cs);

        // As this is called when lines are collapsed, need to strip any -- comments out completely
        int localIndex = curIndex;
        int localQuoteChar = quoteChar;
        while(localIndex < buffer.length()) {
            int c = buffer.charAt(localIndex);
            if(localQuoteChar == UNSET) {
                if(isQuote(c)) {
                    localQuoteChar = c;
                } else if((c == '-') && (localIndex > 0) && (cs.charAt(localIndex - 1) == '-')) {
                    // Found comment, remove to end
                    buffer.delete(localIndex - 1, buffer.length());
                }
            } else if(c == localQuoteChar) {
                localQuoteChar = UNSET;
            }
            ++localIndex;
        }
    }

    public boolean hasQuery() {
        while(curIndex < buffer.length()) {
            char c = buffer.charAt(curIndex);
            if(c == ';') {
                if(quoteChar == UNSET) {
                    endIndex = curIndex;
                    break;
                }
            } else if(isQuote(c)) {
                if(quoteChar == UNSET) {
                    quoteChar = c;
                } else if(quoteChar == c) {
                    quoteChar = UNSET;
                }
            } else if(c == '\\' && isOnlySpace) {
                isBackslash = true;
                startIndex = curIndex;
                endIndex = buffer.length() - 1;
            }
            // After backslash test so preceding whitespace is allowed
            if(isOnlySpace && !Character.isWhitespace(c)) {
                isOnlySpace = false;
            }
            ++curIndex;
        }
        return (endIndex != UNSET);
    }

    public boolean isBackslash() {
        return isBackslash;
    }

    public void setConsumeRemaining() {
        endIndex = buffer.length() - 1;
    }

    public String nextQuery() {
        if(endIndex == UNSET) {
            throw new IllegalArgumentException("No query present");
        }
        String q = buffer.substring(startIndex, ++endIndex);
        reset(endIndex, UNSET, curIndex + 1, buffer.length());
        return q;
    }

    public String trimCompleted() {
        String s = buffer.substring(0, startIndex);
        buffer.delete(0, startIndex);
        reset(0, UNSET, 0, buffer.length());
        return s;
    }

    public boolean isEmpty() {
        return buffer.length() == 0;
    }

    public int length() {
        return buffer.length();
    }

    public void reset() {
        reset(0, UNSET, 0, 0);
    }

    public boolean hasNonSpace() {
        for(int i = 0; i < buffer.length(); ++i) {
            if(!Character.isWhitespace(buffer.charAt(i))) {
                return true;
            }
        }
        return false;
    }

    @Override
    public String toString() {
        return buffer.toString();
    }

    private void reset(int start, int end, int cur, int length) {
        curIndex = cur;
        startIndex = start;
        endIndex = end;
        quoteChar = UNSET;
        isOnlySpace = true;
        isBackslash = false;
        buffer.setLength(length);
    }

    private static boolean isQuote(int c) {
        return c == '\'' | c == '"' | c == '`';
    }
}
