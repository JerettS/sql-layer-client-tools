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

import java.util.ArrayList;
import java.util.List;

public class BackslashParser
{
    private static final int UNSET = -1;

    public static class Parsed {
        public String command;
        public boolean isSystem;
        public boolean isDetail;
        public List<String> args = new ArrayList<>();

        public String argOr(int index, String defValue) {
            return (index >= 0 && index < args.size()) ? args.get(index) : defValue;
        }
    }

    public static Parsed parseFrom(String input) {
        return parseFrom(input, true);
    }

    public static Parsed parseFrom(String input, boolean periodSplits) {
        if(input.isEmpty() || input.charAt(0) != '\\') {
            throw new IllegalArgumentException();
        }

        int i = 0;
        int cmdStart = ++i;
        while((i < input.length()) && !Character.isWhitespace(input.charAt(i))) {
            ++i;
        }

        Parsed parsed = new Parsed();
        parsed.isSystem = isChar(input, i - 1, 'S') || isChar(input, i - 2, 'S');
        parsed.isDetail = isChar(input, i - 1, '+') || isChar(input, i - 2, '+');
        int cmdEnd = i - (parsed.isSystem ? 1 : 0) - (parsed.isDetail ? 1 : 0);
        parsed.command = input.substring(cmdStart, cmdEnd);

        // Split arguments by whitespace or period outside of double quotes
        int nonSpace = UNSET;
        int lastQuote = UNSET;
        while(i < input.length()) {
            char c = input.charAt(i);
            if(c == '"') {
                if(lastQuote == UNSET) {
                    lastQuote = i;
                } else {
                    c = ' ';
                    nonSpace = lastQuote + 1;
                    lastQuote = UNSET;
                }
            }
            if(Character.isWhitespace(c) || (periodSplits && c == '.')) {
                if((lastQuote == UNSET) && (nonSpace != UNSET)) {
                    String arg = input.substring(nonSpace, i);
                    parsed.args.add(arg);
                    nonSpace = -1;
                }
            } else if(nonSpace == UNSET) {
                nonSpace = i;
            }
            ++i;
        }
        if(nonSpace != -1) {
            parsed.args.add(input.substring(nonSpace));
        }
        return parsed;
    }

    private static boolean isChar(String s, int index, char c) {
        return (index >= 0 && index < s.length()) && (s.charAt(index) == c);
    }
}
