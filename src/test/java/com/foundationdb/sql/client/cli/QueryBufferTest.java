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

import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class QueryBufferTest
{
    private QueryBuffer qb = new QueryBuffer();

    @Test
    public void empty() {
        assertEquals(false, qb.hasQuery());
    }

    @Test(expected=IllegalArgumentException.class)
    public void popEmpty() {
        qb.nextQuery();
    }

    @Test
    public void partial() {
        qb.append("SELECT * FROM");
        assertEquals(false, qb.hasQuery());
    }

    @Test(expected=IllegalArgumentException.class)
    public void popPartial() {
        qb.append("SELECT * FROM");
        qb.nextQuery();
    }

    @Test
    public void single() {
        String q = "SELECT * FROM foo;";
        qb.append(q);
        assertEquals(true, qb.hasQuery());
        assertEquals(q, qb.nextQuery());
        assertEquals(false, qb.hasQuery());
    }

    @Test
    public void singleAndPartial() {
        String q1 = "SELECT * FROM foo;";
        String q2 = "SELECT * FROM bar";
        qb.append(q1);
        qb.append(q2);
        assertEquals(true, qb.hasQuery());
        assertEquals(q1, qb.nextQuery());
        assertEquals(false, qb.hasQuery());
        qb.append(";");
        assertEquals(true, qb.hasQuery());
        assertEquals(q2 + ';', qb.nextQuery());
        assertEquals(false, qb.hasQuery());
    }

    @Test
    public void two() {
        String q1 = "SELECT * FROM foo;";
        String q2 = "SELECT * FROM bar;";
        qb.append(q1);
        qb.append(q2);
        assertEquals(true, qb.hasQuery());
        assertEquals(q1, qb.nextQuery());
        assertEquals(true, qb.hasQuery());
        assertEquals(q2, qb.nextQuery());
        assertEquals(false, qb.hasQuery());
    }

    @Test
    public void semiInSingleQuote() {
        String q1 = "SELECT a||';' FROM foo";
        qb.append(q1);
        assertEquals(false, qb.hasQuery());
        qb.append(';');
        assertEquals(true, qb.hasQuery());
        assertEquals(q1 + ';', qb.nextQuery());
        assertEquals(false, qb.hasQuery());
    }

    @Test
    public void semiInDoubleQuote() {
        String q1 = "SELECT a||\";\" FROM foo";
        qb.append(q1);
        assertEquals(false, qb.hasQuery());
        qb.append(';');
        assertEquals(true, qb.hasQuery());
        assertEquals(q1 + ';', qb.nextQuery());
        assertEquals(false, qb.hasQuery());
    }

    @Test
    public void semiInBacktick() {
        String q1 = "SELECT a||`;` FROM foo";
        qb.append(q1);
        assertEquals(false, qb.hasQuery());
        qb.append(';');
        assertEquals(true, qb.hasQuery());
        assertEquals(q1 + ';', qb.nextQuery());
        assertEquals(false, qb.hasQuery());
    }

    @Test
    public void mixedQuotes() {
        String q1 = "SELECT '\";`\"' FROM foo";
        qb.append(q1);
        assertEquals(false, qb.hasQuery());
        qb.append(';');
        assertEquals(true, qb.hasQuery());
        assertEquals(q1 + ';', qb.nextQuery());
        assertEquals(false, qb.hasQuery());
    }

    @Test
    public void appendWithDashComment() {
        // Append does not strip --
        qb.append("SELECT 5; --");
        assertEquals(true, qb.hasQuery());
        assertEquals("SELECT 5;", qb.nextQuery());
        qb.trimCompleted();
        assertEquals(3, qb.length());
    }

    @Test
    public void appendLineJustDashComment() {
        qb.appendLine("--");
        assertEquals(false, qb.hasQuery());
        assertEquals(0, qb.length());
        qb.appendLine("   --");
        assertEquals(false, qb.hasQuery());
        assertEquals(3, qb.length());
    }

    @Test
    public void appendLineDashCommentAfterQuery() {
        qb.appendLine("SELECT 5;-- something");
        assertEquals(true, qb.hasQuery());
        assertEquals("SELECT 5;", qb.nextQuery());
        assertEquals("SELECT 5;", qb.trimCompleted());
        assertEquals(false, qb.hasQuery());
        assertEquals(0, qb.length());
    }

    @Test
    public void appendLineDashInQuote() {
        String q = "SELECT 'foo -- bar';";
        qb.appendLine(q + " -- After");
        assertEquals(true, qb.hasQuery());
        assertEquals(q, qb.nextQuery());
        assertEquals(q, qb.trimCompleted());
        assertEquals(1, qb.length());
    }

    @Test
    public void hasNonSpace() {
        qb.append(' ');
        assertEquals(false, qb.hasNonSpace());
        qb.append('\t');
        assertEquals(false, qb.hasNonSpace());
        qb.append('\n');
        assertEquals(false, qb.hasNonSpace());
        qb.append('a');
        assertEquals(true, qb.hasNonSpace());
        qb.reset();
        assertEquals(false, qb.hasNonSpace());
        qb.append('a');
        assertEquals(true, qb.hasNonSpace());
    }

    @Test
    public void singleBackslash() {
        String b1 = "\\d";
        qb.append(b1);
        assertEquals(true, qb.hasQuery());
        assertEquals(true, qb.isBackslash());
        assertEquals(b1, qb.nextQuery());
        assertEquals(false, qb.hasQuery());
        assertEquals(false, qb.isBackslash());
    }

    @Test
    public void whitespacePrecedingBackslash() {
        String b1 = "    \t\n\r\\d";
        qb.append(b1);
        assertEquals(true, qb.hasQuery());
        assertEquals(true, qb.isBackslash());
        assertEquals("\\d", qb.nextQuery());
        assertEquals(false, qb.hasQuery());
        assertEquals(false, qb.isBackslash());
    }

    @Test
    public void backslashConsumesAll() {
        String b1 = "\\d SELECT * FROM f;";
        qb.append(b1);
        assertEquals(true, qb.hasQuery());
        assertEquals(true, qb.isBackslash());
        assertEquals(b1, qb.nextQuery());
        assertEquals(false, qb.hasQuery());
        assertEquals(false, qb.isBackslash());
    }

    @Test
    public void queryThenBackslash() {
        String q1 = "SELECT * FROM f;";
        String b1 = "\\d t";
        qb.append(q1);
        qb.append(b1);
        assertEquals(true, qb.hasQuery());
        assertEquals(false, qb.isBackslash());
        assertEquals(q1, qb.nextQuery());
        assertEquals(true, qb.hasQuery());
        assertEquals(true, qb.isBackslash());
        assertEquals(b1, qb.nextQuery());
        assertEquals(false, qb.hasQuery());
        assertEquals(false, qb.isBackslash());
    }

    @Test
    public void trimSingleQuery() {
        String q = "SELECT * FROM t;";
        qb.append(q);
        assertEquals(true, qb.hasQuery());
        assertEquals(q, qb.nextQuery());
        assertEquals(false, qb.hasQuery());
        assertEquals(q, qb.trimCompleted());
        assertEquals(false, qb.hasQuery());
        assertEquals(0, qb.length());
    }

    @Test
    public void trimSingleAndPartialQuery() {
        String q1 = "SELECT * FROM t;";
        String q2 = "SELECT * FROM x";
        qb.append(q1);
        qb.append(q2);
        assertEquals(true, qb.hasQuery());
        assertEquals(q1, qb.nextQuery());
        assertEquals(false, qb.hasQuery());
        assertEquals(q1, qb.trimCompleted());
        assertEquals(false, qb.hasQuery());
        assertEquals(q2.length(), qb.length());
        qb.append(';');
        assertEquals(true, qb.hasQuery());
        assertEquals(q2 + ';', qb.nextQuery());
    }

    @Test
    public void trimSingleAndBackslash() {
        String q1 = "SELECT * FROM t;";
        String q2 = "\\d foo";
        qb.append(q1);
        qb.append(q2);
        assertEquals(true, qb.hasQuery());
        assertEquals(q1, qb.nextQuery());
        assertEquals(true, qb.hasQuery());
        assertEquals(q1, qb.trimCompleted());
        assertEquals(true, qb.hasQuery());
        assertEquals(q2, qb.nextQuery());
        assertEquals(q2, qb.trimCompleted());
        assertEquals(0, qb.length());
    }
}
