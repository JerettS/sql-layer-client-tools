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
    private static final Quote DASH_QUOTE = new Quote("--", "\n");
    private static final Quote[] QUOTES = {
        new Quote('\''),
        new Quote('"'),
        new Quote('`'),
        new Quote("$$"),
        new Quote("/*", "*/"),
        DASH_QUOTE
    };

    private final StringBuilder buffer = new StringBuilder();
    private int curIndex;
    private int startIndex;
    private int endIndex;
    private Quote curQuote;
    private boolean isOnlySpace;
    private boolean isBackslash;

    public QueryBuffer() {
        reset();
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

    /** As append() with automatic '--' stripping. */
    public void appendLine(CharSequence cs) {
        append(cs);

        // As this is called when lines are collapsed, need to strip any -- comments out completely
        int localIndex = curIndex;
        Quote localQuote = curQuote;
        while(localIndex < buffer.length()) {
            if(localQuote == null) {
                localQuote = quoteStartAt(buffer, localIndex);
                if(localQuote != null) {
                    localIndex += localQuote.beginSkipLength();
                }
            } else if(quoteEndsAt(buffer, localQuote, localIndex)) {
                localQuote = null;
            }
            if(localQuote == DASH_QUOTE) {
                // Found comment, remove to end
                buffer.delete(localIndex - 1, buffer.length());
            }
            ++localIndex;
        }
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
                        endIndex = buffer.length() - 1;
                    }
                } else if(curQuote == DASH_QUOTE) {
                    // Ignored until we can handle buffer containing newlines
                    curQuote = null;
                } else {
                    curIndex += curQuote.beginSkipLength();
                }
            } else {
                if( curQuote.isBlockQuote && isBlockQuote(buffer, curQuote, curIndex)){
                    curQuote.blockQuoteCount++;
                }
                else if(quoteEndsAt(buffer, curQuote, curIndex)) {
                    curQuote.blockQuoteCount--;
                    if(curQuote.blockQuoteCount <= 0) {
                        curQuote.isBlockQuote = false;
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
        isOnlySpace = true;
        isBackslash = false;
        buffer.setLength(length);
    }

    private static Quote quoteStartAt(StringBuilder sb, int index) {
        for(Quote q : QUOTES) {
            boolean match = true;
            for(int i = 0; (i < q.begin.length()) && (index + i < sb.length()); ++i) {
                match &= (sb.charAt(index +i) == q.begin.charAt(i));
            }
            if(match) {
                if(q == QUOTES[4]){
                    q.isBlockQuote = true;
                    q.blockQuoteCount = 1;
                }
                return q;
            }
        }
        return null;
    }

    private static boolean isBlockQuote(StringBuilder sb, Quote q, int index) {
        for (int i = q.begin.length() - 1, j = index; i >= 0 && j >= 0; --i, --j) {
            if (q.begin.charAt(i) != sb.charAt(j)) {
                return false;
            }
        }
        return true;
    }


    private static boolean quoteEndsAt(StringBuilder sb, Quote q, int index) {
        for(int i = q.end.length() - 1, j = index; i >= 0 && j >= 0; --i, --j) {
            if(q.end.charAt(i) != sb.charAt(j)) {
                return false;
            }
        }
        return true;
    }

    private static class Quote {
        public final String begin;
        public final String end;

        public int blockQuoteCount = 0;
        public boolean isBlockQuote = false;

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
