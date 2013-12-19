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

package com.foundationdb.sql.client.cli;

import org.junit.Test;

import java.util.Arrays;

import static org.junit.Assert.assertEquals;

public class BackslashParserTest
{
    @Test(expected=IllegalArgumentException.class)
    public void empty() {
        BackslashParser.parseFrom("");
    }

    @Test(expected=IllegalArgumentException.class)
    public void noBackslash() {
        BackslashParser.parseFrom("lS+");
    }

    @Test
    public void noArgs() {
        expect("\\l", "l", false, false);
    }

    @Test
    public void multiLetter() {
        expect("\\cmd", "cmd", false, false);
    }

    @Test
    public void tailingSpace() {
        expect("\\l  \t ", "l", false, false);
    }

    @Test
    public void system() {
        expect("\\lS", "l", true, false);
    }

    @Test
    public void detail() {
        expect("\\l+", "l", false, true);
    }

    @Test
    public void systemAndDetail() {
        expect("\\lS+", "l", true, true);
    }

    @Test
    public void detailAndSystem() {
        expect("\\l+S", "l", true, true);
    }

    @Test
    public void multipleSystem() {
        expect("\\lSS", "lS", true, false);
    }

    @Test
    public void oneArg() {
        expect("\\l test", "l", false, false, "test");
    }

    @Test
    public void oneArgTrailingSpace() {
        expect("\\l test ", "l", false, false, "test");
    }

    @Test
    public void twoArg() {
        expect("\\l test foo", "l", false, false, "test", "foo");
    }

    @Test
    public void quotedArg() {
        expect("\\l \"te st\"", "l", false, false, "te st");
    }

    @Test
    public void unclosedQuote() {
        expect("\\l \"test", "l", false, false, "\"test");
    }

    @Test
    public void endOpenQuote() {
        expect("\\l test\"", "l", false, false, "test\"");
    }

    @Test
    public void multiArgAndQuote() {
        expect("\\l \"te st\" a g \t \"  \t  \"", "l", false, false, "te st", "a", "g", "  \t  ");
    }

    @Test
    public void singleQualified() {
        expect("\\l test.foo", "l", false, false, "test", "foo");
    }

    @Test
    public void twoQualified() {
        expect("\\l test.foo bar.zap", "l", false, false, "test", "foo", "bar", "zap");
    }

    @Test
    public void quotedQualified() {
        expect("\\l \"te.st\".foo", "l", false, false, "te.st", "foo");
    }

    @Test
    public void multiArgQuotedAndQualified() {
        expect("\\l \"te st.\".\".f\"oo \"b.ar\"", "l", false, false, "te st.", ".f", "oo", "b.ar");
    }

    @Test
    public void systemDetailQuotedQualified() {
        expect("\\lvS+ \"test\".\"foo\"", "lv", true, true, "test", "foo");
    }


    private static void expect(String input, String command, boolean isSystem, boolean isDetail, String... args) {
        BackslashParser.Parsed actual = BackslashParser.parseFrom(input);
        assertEquals("command", command, actual.command);
        assertEquals("isSystem", isSystem, actual.isSystem);
        assertEquals("isDetail", isDetail, actual.isDetail);
        assertEquals("args", Arrays.asList(args), actual.args);
    }
}
