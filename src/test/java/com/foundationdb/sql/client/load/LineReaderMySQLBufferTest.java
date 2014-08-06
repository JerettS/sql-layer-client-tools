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
import static org.junit.Assert.*;

public class LineReaderMySQLBufferTest {
    static final String encoding = "UTF-8";
    FileInputStream inputStream;

    static final ArrayList<MySQLBuffer.Query> emptyQueries = new ArrayList<>();

    @Test
    public void testSimpleReadLineComment() throws IOException {
        // this is how mysql dumps start
        assertReadLines(emptyQueries,
                        "-- MySQL dump 10.13  Distrib 5.5.28, for debian-linux-gnu (x86_64)");
    }

    @Test
    public void testSimpleReadBrokenLineComment() throws IOException {
        assertUnexpectedToken('-', ' ', "- - broken comment");
    }

    @Test
    public void testDelimitedComment() throws IOException {
        assertReadLines(emptyQueries,
                        "/*!40101 SET @OLD_CHARACTER_SET_CLIENT=@@CHARACTER_SET_CLIENT */");
    }

    @Test
    public void testDelimitedCommentFailedStart() throws IOException {
        assertUnexpectedToken('*','x',"/x");
    }

    @Test
    public void testDelimitedCommentWithFunkyStuff() throws IOException {
        // by funky I mean * / ;
        assertReadLines(emptyQueries,
                        "/*!40101 SET @OLD_C * HAR;ACTER_SE / T_C;LI/ENT=@@CHARA*CTER_SET_CLIENT */");
    }

    @Test
    public void testMultilineDelimitedCommentWithFunkyStuff() throws IOException {
        // by funky I mean * / ;
        assertReadLines(emptyQueries,
                        "/*!40101 SET @OLD_C * \nHAR;ACTER_SE / T_\nC;LI/ENT=@@CHARA*CTER_SET_CLIENT */");
    }

    @Test
    public void testEmptyStatement() throws IOException {
        assertReadLines(emptyQueries, ";");
    }

    @Test
    public void testDelimitedCommentWithStraySemicolon() throws IOException {
        assertReadLines(emptyQueries, "/* a comment */;");
    }

    // TODO @Test public void testSingleLineCommentThenStatement()

    // TODO @Test public void testDelimitedCommentThenStatement()

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
                assertEquals("an UnexpectedTokenException to be thrown", "not thrown");
            } catch (MySQLBuffer.UnexpectedTokenException e) {
                assertEquals(expected, e.getExpected());
                assertEquals(actual, e.getActual());
            }
        } finally {
            if (istr != null) {
                istr.close();
            }
        }
    }

    private static void assertRows(List<MySQLBuffer.Query> expected, MySQLBuffer buffer, LineReader lines) throws IOException {
        List<MySQLBuffer.Query> actual = new ArrayList<>();
        while (lines.readLine(buffer)) {
            actual.add(buffer.nextQuery());
        }
        assertArrayEquals(expected.toArray(), actual.toArray());
    }

}
