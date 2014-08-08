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

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.List;

import org.junit.Test;

import static com.foundationdb.sql.client.load.LineReaderCsvBufferTest.tmpFileFrom;
import static com.foundationdb.sql.client.load.LineReaderCsvBufferTest.list;
import static org.junit.Assert.*;
import static org.hamcrest.CoreMatchers.instanceOf;

public class LineReaderMySQLBufferTest {
    static final String encoding = "UTF-8";
    FileInputStream inputStream;

    @Test
    public void testSimpleReadLineComment() throws IOException {
        // this is how mysql dumps start
        assertReadLines("-- MySQL dump 10.13 /* ' \" ` Dist;rib 5.5.28, for debian-linux-gnu (x86_64)");
    }

    @Test
    public void testSimpleReadBrokenLineComment() throws IOException {
        assertUnexpectedToken('-', ' ', "- - broken comment");
    }

    @Test
    public void testDelimitedComment() throws IOException {
        assertReadLines("/*!40101 SET @OLD_CHARACTER_SET_CLIENT=@@CHARACTER_SET_CLIENT */");
    }

    @Test
    public void testDelimitedCommentFailedStart() throws IOException {
        assertUnexpectedToken('*','x',"/x");
    }

    @Test
    public void testDelimitedCommentWithFunkyStuff() throws IOException {
        // by funky I mean * / ;
        assertReadLines("/*!40101 SET @OLD_C * HAR;ACTER_SE / T_C;LI/ENT=@@CHARA*CTER_SET_CLIENT */");
    }

    @Test
    public void testMultilineDelimitedCommentWithFunkyStuff() throws IOException {
        // by funky I mean * / ; " ' `
        assertReadLines("/*!40101 SET @OLD_C * \nHAR;ACTER_SE / T_\nC;LI/E`NT=@@C\"HARA*CTER_SET_C'LIENT */");
    }

    @Test
    public void testEmptyStatement() throws IOException {
        assertReadLines(";");
    }

    @Test
    public void testDelimitedCommentWithStraySemicolon() throws IOException {
        assertReadLines("/* a comment */;");
    }

    @Test
    public void testSingleLineCommentThenStatement() throws Exception {
        assertReadLines(query("INSERT INTO \"t\" VALUES (?)", "1"),
                        "-- INSERT INTO t VALUES (5);",
                        "INSERT INTO t VALUES (1);");
    }

    @Test
    public void testDelimitedCommentThenStatement() throws Exception {
        assertReadLines(query("INSERT INTO \"t\" VALUES (?)", "83"),
                        "/*INSERT INTO t VALUES (5);",
                        "Insert into t VALUES (9);*/",
                        "INSERT INTO t VALUES (83);");
    }

    @Test
    public void testIgnoredLockStatement() throws IOException {
        assertReadLines("LOCK INSERT INTO TABLE t VALUES (1,3);");
    }

    @Test
    public void testIgnoredUnlockStatement() throws IOException {
        assertReadLines("UNLOCK INSERT INTO TABLE t VALUES (1,3);");
    }

    @Test
    public void testBadCharacterInVerb() throws Exception {
        assertUnexpectedToken("a letter", '3', "LOC3");
    }

    @Test
    public void testUnexpectedVerb() throws Exception {
        MySQLBuffer.UnexpectedKeyword e = (MySQLBuffer.UnexpectedKeyword)returnException("WHATEVER you want to do;");
        assertEquals("WHATEVER", e.getActual());
    }

    @Test
    public void testBackQuotesInIgnoredStatement() throws Exception {
        assertReadLines("LOCK `;INSERT INTO FOO` yadda;");
    }

    @Test
    public void testSingleQuotesInIgnoredStatement() throws Exception {
        assertReadLines("LOCK ';INSERT INTO FOO' yadda;");
    }

    @Test
    public void testDoubleQuotesInIgnoredStatement() throws Exception {
        assertReadLines("LOCK \";INSERT INTO FOO\" yadda;");
    }

    @Test
    public void testBackQuotesEscapedIgnoredStatement() throws Exception {
        assertReadLines("LOCK ` wooo \\`;INSERT INTO FOO` yadda;");
    }

    @Test
    public void testSingleQuotesEscapedIgnoredStatement() throws Exception {
        assertReadLines("LOCK ' wooo \\';INSERT INTO FOO' yadda;");
    }

    @Test
    public void testDoubleQuotesEscapedIgnoredStatement() throws Exception {
        assertReadLines("LOCK \" wooo \\\";INSERT INTO FOO\" yadda;");
    }

    @Test
    public void testDoubleQuotesEscapedBackslashIgnoredStatement() throws Exception {
        assertReadLines("LOCK \" wooo \\\\\\\\\\\";INSERT INTO FOO\" yadda;");
    }

    @Test
    public void testIgnoredLockStatementThenRealStatement() throws Exception {
        assertReadLines(query("INSERT INTO \"t\" VALUES (?)", "1"),
                        "LOCK TABLES `t1` WRITE;",
                        "INSERT INTO t VALUES (1);");
    }

    @Test
    public void testLeadingWhitespace() throws Exception {
        assertReadLines(query("INSERT INTO \"t\" VALUES (?)", "TRUE"),
                        "  \r  \n INSERT INTO t VALUES (TRUE);");
    }

    @Test
    public void testInsertGibberish() throws Exception {
        MySQLBuffer.UnexpectedKeyword e = (MySQLBuffer.UnexpectedKeyword)returnException("INSERT foobar is cool");
        assertEquals("foobar", e.getActual());
        assertEquals("INTO", e.getExpected());
    }

    @Test
    public void testInsertWithColumns() throws Exception {
        assertUnexpectedToken("a letter", '(', "INSERT into t (a,b) VALUES (3,4);");
    }

    @Test
    public void testSimpleInsert() throws Exception {
        assertReadLines(query("INSERT INTO \"t\" VALUES (?)", "1"), "INSERT INTO t VALUES (1);");
    }

    @Test
    public void testChallengingTableName() throws Exception {
        assertReadLines(query("INSERT INTO \"tY3_$\u0080\" VALUES (?)", "1"), "INSERT INTO tY3_$\u0080 VALUES (1);");
    }

    @Test
    public void testInsertQuotedTableName() throws Exception {
        assertReadLines(query("INSERT INTO \"t\" VALUES (?)", "1"), "INSERT INTO `t` VALUES (1);");
    }

    @Test
    public void testInsertQuotedTableNameEscapes() throws Exception {
        assertReadLines(query("INSERT INTO \"t` worst \"\"table\"\" name \\ever \" VALUES (?)", "1"),
                        "INSERT INTO `t\\` worst \\\"table\\\" name \\\\ever ` VALUES (1);");
    }

    @Test
    public void testInsertMultipleCharacterField() throws Exception {
        assertReadLines(query("INSERT INTO \"t\" VALUES (?)", "173"), "INSERT INTO t VALUES (173);");
    }

    @Test
    public void testInsertMultipleColumns() throws Exception {
        assertReadLines(query("INSERT INTO \"t\" VALUES (?, ?)", "18", "173"), "INSERT INTO t VALUES (18, 173);");
    }

    @Test
    public void testSingleQuotedField() throws Exception {
        assertReadLines(query("INSERT INTO \"t\" VALUES (?)", "boo is cool"), "INSERT INTO t VALUES ('boo is cool');");
    }

    @Test
    public void testDoubleQuotedField() throws Exception {
        assertReadLines(query("INSERT INTO \"t\" VALUES (?)", "boo is cool"), "INSERT INTO t VALUES (\"boo is cool\");");
    }

    @Test
    public void testSingleQuotedFieldFunkyStuff() throws Exception {
        assertReadLines(query("INSERT INTO \"t\" VALUES (?)", "boo is ' \" \\ % _ ' \" ,); a 7 z + cool"),
                        "INSERT INTO t VALUES ('boo is \\' \\\" \\\\ \\% \\_ '' \" ,); \\a \\7 \\z \\+ cool');");
    }

    @Test
    public void testDoubleQuotedFieldWithFunkyStuff() throws Exception {
        assertReadLines(query("INSERT INTO \"t\" VALUES (?)", "boo is ' \" \\ % _ \" ' ,); a 7 z + cool"),
                        "INSERT INTO t VALUES (\"boo is \\' \\\" \\\\ \\% \\_ \"\" ' ,); \\a \\7 \\z \\+ cool\");");
    }

    // TODO do one of these tests through the sql layer
    @Test
    public void testSingleQuotedFieldFunkyStuff2() throws Exception {
        assertReadLines(query("INSERT INTO \"t\" VALUES (?)", "boo is \u0000 \b \n \r \t \u001A cool"),
                        "INSERT INTO t VALUES ('boo is \\0 \\b \\n \\r \\t \\Z cool');");
    }

    @Test
    public void testDoubleQuotedFieldWithFunkyStuff2() throws Exception {
        assertReadLines(query("INSERT INTO \"t\" VALUES (?)", "boo is \u0000 \b \n \r \t \u001A cool"),
                        "INSERT INTO t VALUES (\"boo is \\0 \\b \\n \\r \\t \\Z cool\");");
    }

    private static MySQLBuffer.Query query(String prepared, String... values) {
        return new MySQLBuffer.Query(prepared, values);
    }

    private static void assertReadLines(String... input) throws IOException {
        assertReadLines(true, new ArrayList<MySQLBuffer.Query>(), input);
    }

    private static void assertReadLines(MySQLBuffer.Query expected, String... input) throws IOException {
        assertReadLines(true, list(expected), input);
    }

    private static void assertReadLines(List<MySQLBuffer.Query> expected, String... input) throws IOException {
        assertReadLines(true, expected, input);
    }

    private static void assertReadLines(boolean insertNewlines, List<MySQLBuffer.Query> expected, String... input) throws IOException {
        File file = tmpFileFrom(insertNewlines, input);
        FileInputStream istr = null;
        try {
            istr = new FileInputStream(file);
            LineReader lines = new LineReader(istr.getChannel(), encoding, 1);
            MySQLBuffer b = new MySQLBuffer(encoding);
            assertRows(expected, b, lines);
        } finally {
            if (istr != null) {
                istr.close();
            }
        }
    }

    private static void assertUnexpectedToken(char expected, char actual, String... input) throws IOException {
        assertUnexpectedToken("'" + expected + "'", actual, input);
    }

    private static void assertUnexpectedToken(String expected, char actual, String... input) throws IOException {
        Exception e = returnException(input);
        assertThat(e,instanceOf(MySQLBuffer.UnexpectedTokenException.class));
        MySQLBuffer.UnexpectedTokenException ute = (MySQLBuffer.UnexpectedTokenException) e;
        assertEquals(expected, ute.getExpected());
        assertEquals(actual, ute.getActual());
    }

    private static Exception returnException(String... input) throws IOException {
        File file = tmpFileFrom(true, input);
        FileInputStream istr = null;
        try {
            istr = new FileInputStream(file);
            LineReader lines = new LineReader(istr.getChannel(), encoding, 1);
            MySQLBuffer buffer = new MySQLBuffer(encoding);
            try {
                while (lines.readLine(buffer)) {
                    buffer.nextQuery();
                }
                assertEquals("an Exception to be thrown", "not thrown");
            } catch (Exception e) {
                return e;
            }
        } finally {
            if (istr != null) {
                istr.close();
            }
        }
        throw new RuntimeException("returnException is broken");
    }

    private static void assertRows(List<MySQLBuffer.Query> expected, MySQLBuffer buffer, LineReader lines) throws IOException {
        List<MySQLBuffer.Query> actual = new ArrayList<>();
        while (lines.readLine(buffer)) {
            actual.add(buffer.nextQuery());
        }
        assertArrayEquals(expected.toArray(), actual.toArray());
    }

}
