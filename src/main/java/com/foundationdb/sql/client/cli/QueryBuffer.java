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
 * <p>Backslash queries consume up to a newline, or end of buffer, and are found with optional whitespace preceding a backslash.</p>
 * <p>Semi-colon delimited splits on un-quoted (', " or `) semi-colons with any trailing remaining in buffer.</p>
 */
public class QueryBuffer
{
    private static final int UNSET = -1;
    private static final Quote DASH_QUOTE = new Quote("--", "\n");
    private static final Quote ESTRING_QUOTE = new Quote("E'", "'");
    private static final Quote BLOCK_QUOTE = new Quote("/*", "*/");
    private static final Quote[] QUOTES = {
        // Note: Ordering sensitive
        ESTRING_QUOTE,
        new Quote('\''),
        new Quote('"'),
        new Quote('`'),
        new Quote("$$"),
        DASH_QUOTE,
        BLOCK_QUOTE,
    };

    private final StringBuilder buffer = new StringBuilder();
    private int curIndex;
    private int startIndex;
    private int endIndex;
    private Quote curQuote;
    private int curQuoteStart;
    private boolean isOnlySpace;
    private boolean isBackslash;
    private boolean stripDashQuote;
    private int blockQuoteCount = 0;

    public QueryBuffer() {
        reset();
    }

    boolean inQuote() {
        return curQuote != null;
    }

    String getQuote() {
        return curQuote.begin;
    }

    public void setStripDashQuote() {
        stripDashQuote = true;
    }

    public String quoteString() {
        return (curQuote != null) ? curQuote.begin : "";
    }

    public void append(char c) {
        buffer.append(c);
    }

    public void append(CharSequence cs) {
        buffer.append(cs);
    }

    public boolean hasQuery() {
        while(curIndex < buffer.length()) {
            char c = buffer.charAt(curIndex);
            if(curQuote == null) {
                curQuote = quoteStartAt(buffer, curIndex);
                if(curQuote == null) {
                    if(c == ';') {
                        endIndex = curIndex;
                        break;
                    } else if(c == '\\' && isOnlySpace) {
                        isBackslash = true;
                        startIndex = curIndex;
                        endIndex = buffer.indexOf("\n", curIndex) - 1;
                        if(endIndex < 0) {
                            endIndex = buffer.length() - 1;
                        }
                        curIndex = endIndex;
                        break;
                    } else if(c == '\n' && isOnlySpace) {
                        ++startIndex;
                    }
                } else {
                    if(curQuote == BLOCK_QUOTE) {
                        blockQuoteCount = 1;
                    }
                    curQuoteStart = curIndex - curQuote.begin.length() + 1;
                    curIndex += curQuote.beginSkipLength();
                }
            } else {
                if((curQuote == BLOCK_QUOTE) && matchesBehind(buffer, curIndex, BLOCK_QUOTE.begin)) {
                    blockQuoteCount++;
                } else if(quoteEndsAt(buffer, curQuote, curIndex)) {
                    if(curQuote == BLOCK_QUOTE) {
                        blockQuoteCount--;
                    }
                    if((curQuote == DASH_QUOTE) && stripDashQuote) {
                        buffer.delete(curQuoteStart, curIndex + 1);
                        curIndex = curQuoteStart - 1;
                    }
                    if(blockQuoteCount == 0) {
                        curQuote = null;
                    }
                }
            }
            // Backslash may only be preceded by whitespace
            isOnlySpace &= Character.isWhitespace(c);
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
        if(startIndex > 0) {
            buffer.delete(0, startIndex);
            reset(0, UNSET, 0, buffer.length());
        }
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
        curQuote = null;
        curQuoteStart = UNSET;
        isOnlySpace = true;
        isBackslash = false;
        buffer.setLength(length);
    }

    private static Quote quoteStartAt(StringBuilder sb, int index) {
        for(Quote q : QUOTES) {
            if(matchesBehind(sb, index, q.begin)) {
                return q;
            }
        }
        return null;
    }

    // Note: A quote quote is handled as quote end and quote start. OK as we don't return tokens.
    private static boolean quoteEndsAt(StringBuilder sb, Quote q, int index) {
        if(!matchesBehind(sb, index, q.end)) {
            return false;
        }
        // Delicate: E strings can contain backslash-quote, which shouldn't end the quote.
        if((ESTRING_QUOTE == q) && (index > 0) && (sb.charAt(index - 1) == '\\')) {
            // And neither should backslash-backslash quote-quote
            if((index < 2) || (sb.charAt(index - 2) != '\\')) {
                return false;
            }
        }
        return true;
    }

    private static boolean matchesBehind(StringBuilder sb, int index, String m) {
        int j = index - m.length() + 1;
        if(j < 0) {
            return false;
        }
        for(int i = 0; i < m.length(); ++i, ++j) {
            if(m.charAt(i) != sb.charAt(j)) {
                return false;
            }
        }
        return true;
    }

    private static class Quote {
        public final String begin;
        public final String end;

        private Quote(char quote) {
            this(String.valueOf(quote));
        }

        private Quote(String quote) {
            this(quote, quote);
        }

        private Quote(String begin, String end) {
            assert begin.length() <= 2;
            assert end.length() <= 2;
            this.begin = begin;
            this.end = end;
        }

        public int beginSkipLength() {
            return begin.length() - 1;
        }

        @Override
        public String toString() {
            return begin;
        }
    }
}
