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
import java.io.FileWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.junit.Test;

import static org.junit.Assert.*;

public class LineReaderCsvBufferTest {
    static final String encoding = "UTF-8";
    FileInputStream inputStream;

    @Test
    public void simpleReadOneColumn() throws Exception {
        assertReadLines(Arrays.asList(Arrays.asList("field1")), "field1");
    }

    @Test
    public void simpleRead() throws Exception {
        assertReadLines(Arrays.asList(Arrays.asList("field1", "field2")), "field1,field2");
    }

    @Test
    public void simpleReadMissingTrailingNewline() throws Exception {
        assertReadLines(false, Arrays.asList(Arrays.asList("field1", "field2")), "field1,field2");
    }

    @Test
    public void simpleReadTwoRows() throws Exception {
        assertReadLines(Arrays.asList(Arrays.asList("field1", "field2"), Arrays.asList("field3", "field4")), "field1,field2","field3,field4");
    }

    @Test
    public void simpleMissingTrailingNewlineReadTwoRows() throws Exception {
        assertReadLines(false, Arrays.asList(Arrays.asList("field1", "field2"), Arrays.asList("field3", "field4")), "field1,field2\n","field3,field4");
    }

    @Test
    public void simpleQuoted() throws Exception {
        assertReadLines(Arrays.asList(Arrays.asList("a field", "field2")), "\"a field\",field2");
    }

    @Test
    public void simpleQuotedDelimiter() throws Exception {
        assertReadLines(Arrays.asList(Arrays.asList("a,field", "field2")), "\"a,field\",field2");
    }

    @Test
    public void quotedNewline() throws Exception {
        assertReadLines(Arrays.asList(Arrays.asList("a\nfield", "field2")), "\"a\nfield\",field2");
    }

    @Test
    public void quotedCarriageReturn() throws Exception {
        assertReadLines(Arrays.asList(Arrays.asList("a\rfield", "field2")), "\"a\rfield\",field2");
    }

    @Test
    public void quotedQuote() throws Exception {
        assertReadLines(Arrays.asList(Arrays.asList("a field", "the \"second\" field")), "a field,\"the \"\"second\"\" field\"");
    }

    @Test
    public void testSplit() throws Exception {
        String line1 = "first row,has the value,3";
        String line2 = "second row,has the value,17";
        String line3 = "third row,has the value,950";
        int fullLength = line1.length() + line2.length() + line3.length();
        int split1 = fullLength / 3;
        int split2 = split1 * 2;
        assertNotEquals("Don't make it too easy", split1, line1.length());
        File file = tmpFileFrom(true, line1, line2, line3);
        try (FileInputStream istr = new FileInputStream(file)) {
            // NOTE: right now the char buffer size must be 1 for calling splitParse
            LineReader lines = new LineReader(istr.getChannel(), encoding, 1);
            long splitPoint = lines.splitParse(split1, new CsvBuffer());
            long splitPoint2 = lines.splitParse(split2, new CsvBuffer());
            lines = new LineReader(istr.getChannel(), encoding, FileLoader.SMALL_BUFFER_SIZE, 128, 0, splitPoint);
            CsvBuffer csv = new CsvBuffer();
            assertRows(Arrays.asList(Arrays.asList("first row", "has the value", "3")), csv, lines);
            lines = new LineReader(istr.getChannel(), encoding, FileLoader.SMALL_BUFFER_SIZE, 128, splitPoint, splitPoint2);
            csv = new CsvBuffer();
            assertRows(Arrays.asList(Arrays.asList("second row", "has the value", "17")), csv, lines);
            lines = new LineReader(istr.getChannel(), encoding, FileLoader.SMALL_BUFFER_SIZE, 128, splitPoint2, istr.getChannel().size());
            csv = new CsvBuffer();
            assertRows(Arrays.asList(Arrays.asList("third row", "has the value", "950")), csv, lines);
        }
    }

    private static void assertReadLines(List<List<String>> expected, String... input) throws Exception {
        assertReadLines(true, expected, input);
    }

    private static void assertReadLines(boolean insertNewlines, List<List<String>> expected, String... input) throws Exception {
        File file = tmpFileFrom(insertNewlines, input);
        FileInputStream istr = null;
        try {
            istr = new FileInputStream(file);
            LineReader lines = new LineReader(istr.getChannel(), encoding, 1);
            CsvBuffer b = new CsvBuffer();
            assertRows(expected, b, lines);
        } finally {
            if (istr != null) {
                istr.close();
            }
        }
    }

    private static void assertRows(List<List<String>> expected, CsvBuffer buffer, LineReader lines) throws Exception {
        List<List<String>> actual = new ArrayList<>();
        while (lines.readLine(buffer)) {
            actual.add(buffer.nextStatement());
        }
        assertArrayEquals(expected.toArray(), actual.toArray());
    }

    public static File tmpFileFrom(boolean insertNewlines, String... lines) throws Exception {
        File tmpFile = File.createTempFile(LineReaderQueryBufferTest.class.getSimpleName(), null);
        tmpFile.deleteOnExit();
        FileWriter writer = new FileWriter(tmpFile);
        for(String l : lines) {
            writer.write(l);
            if (insertNewlines) {
                writer.write('\n');
            }
        }
        writer.flush();
        writer.close();
        return tmpFile;
    }
}
