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

import org.junit.Before;
import org.junit.Test;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;

import static org.hamcrest.CoreMatchers.containsString;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

public class CsvLoaderTest extends LoaderTestBase
{
    @Before
    public void setupOptions() {
        super.setupOptions();
        options.format = Format.CSV;
        options.target = "states";
    }

    @Test
    public void testBasicLoad() throws Exception {
        loadDDL("DROP TABLE IF EXISTS states",
                "CREATE TABLE states(abbrev CHAR(2) PRIMARY KEY, name VARCHAR(128))");
        assertLoad(2, "AL,Birmingham","MA,Boston");
        checkQuery("SELECT * FROM states", Arrays.asList(Arrays.asList((Object) "AL", "Birmingham"), Arrays.asList((Object) "MA", "Boston")));
    }

    @Test
    public void testEscapeTableName() throws Exception {
        String escapedTable = "\"the ; , \"\" bad ; , ? ? states\"";
        loadDDL("DROP TABLE IF EXISTS " + escapedTable,
                "CREATE TABLE " + escapedTable + " (abbrev CHAR(2) PRIMARY KEY, name VARCHAR(128))");
        options.target = "the ; , \" bad ; , ? ? states";
        assertLoad(2, "AL,Birmingham","MA,Boston");
        checkQuery("SELECT * FROM " + escapedTable, Arrays.asList(Arrays.asList((Object) "AL", "Birmingham"), Arrays.asList((Object) "MA", "Boston")));
    }

    @Test
    public void testEscapeColumnName() throws Exception {
        String escapedColumnName = "\"the ; , \"\" bad ; , ? ? abbreviation\"";
        loadDDL("DROP TABLE IF EXISTS states ",
                "CREATE TABLE states (" + escapedColumnName + " CHAR(2) PRIMARY KEY, name VARCHAR(128))");
        assertLoad(2, "AL,Birmingham","MA,Boston");
        checkQuery("SELECT * FROM states", Arrays.asList(Arrays.asList((Object) "AL", "Birmingham"), Arrays.asList((Object) "MA", "Boston")));
    }

    @Test
    public void testEscapeColumnNameWithHeader() throws Exception {
        String escapedColumnName = "\"the (; ,) \"\" bad ; , ? ? abbreviation\"";
        loadDDL("DROP TABLE IF EXISTS states ",
                "CREATE TABLE states (" + escapedColumnName + " CHAR(2) PRIMARY KEY, name VARCHAR(128))");
        options.format = Format.CSV_HEADER;
        // conveniently csv & sql escape in the same way for table/column names
        assertLoad(2, escapedColumnName + ",name", "AL,Birmingham","MA,Boston");
        checkQuery("SELECT * FROM states", Arrays.asList(Arrays.asList((Object) "AL", "Birmingham"), Arrays.asList((Object) "MA", "Boston")));
    }

    @Test
    public void testEmptyCsvWithoutHeader() throws Exception {
        // Note: Exception will be caught by main() and the message will be printed out
        try {
            assertLoad(0, "");
            fail("No exception was thrown");
        } catch (IndexOutOfBoundsException e) {
            assertEquals("Csv file is empty", e.getMessage());
        }
    }

    @Test
    public void testEmptyCsvWithHeader() throws Exception {
        // Note: Exception will be caught by main() and the message will be printed out
        try {
            assertLoad(0, "");
            fail("No exception was thrown");
        } catch (IndexOutOfBoundsException e) {
            assertEquals("Csv file is empty", e.getMessage());
        }
    }

    @Test
    public void testTooManyColumnsInHeader() throws Exception {
        expectsErrorOutput = true;
        loadDDL("DROP TABLE IF EXISTS states ",
                "CREATE TABLE states (abbrev CHAR(2) PRIMARY KEY, name VARCHAR(128))");
        options.format = Format.CSV_HEADER;
        assertLoad(0, "abbrev,name,other", "AL,Birmingham","MA,Boston");
        assertThat(errorStream.toString(),
                   containsString("No value specified for parameter 3."));
    }

    @Test
    public void testTooManyColumnsWithoutHeader() throws Exception {
        expectsErrorOutput = true;
        loadDDL("DROP TABLE IF EXISTS states ",
                "CREATE TABLE states (abbrev CHAR(2) PRIMARY KEY, name VARCHAR(128))");
        assertLoad(0, "AL,Birmingham,USA","MA,Boston");
        assertThat(errorStream.toString(),
                   containsString("Number of target columns (2) is not the same as number of expressions (3)"));
    }

    @Test
    public void testTooManyColumnsWithoutHeader2() throws Exception {
        expectsErrorOutput = true;
        loadDDL("DROP TABLE IF EXISTS states ",
                "CREATE TABLE states (abbrev CHAR(2) PRIMARY KEY, name VARCHAR(128))");
        assertLoad(0, "AL,Birmingham","MA,Boston,USA");
        assertThat(errorStream.toString(),
                   containsString("The column index is out of range: 3, number of columns: 2."));
    }

    @Test
    public void testTooFewColumnsInHeader() throws Exception {
        expectsErrorOutput = true;
        loadDDL("DROP TABLE IF EXISTS states ",
                "CREATE TABLE states (abbrev CHAR(2) PRIMARY KEY, name VARCHAR(128))");
        options.format = Format.CSV_HEADER;
        assertLoad(0, "abbrev", "AL,Birmingham","MA,Boston");
        assertThat(errorStream.toString(),
                   containsString("The column index is out of range: 2, number of columns: 1."));
    }

    @Test
    public void testTooFewColumnsWithoutHeader() throws Exception {
        expectsErrorOutput = true;
        loadDDL("DROP TABLE IF EXISTS states ",
                "CREATE TABLE states (abbrev CHAR(2) PRIMARY KEY, name VARCHAR(128))");
        assertLoad(0, "AL","MA,Boston");
        assertThat(errorStream.toString(),
                   containsString("The column index is out of range: 2, number of columns: 1."));
    }

    @Test
    public void testTooFewColumnsWithoutHeader2() throws Exception {
        expectsErrorOutput = true;
        loadDDL("DROP TABLE IF EXISTS states ",
                "CREATE TABLE states (abbrev CHAR(2) PRIMARY KEY, name VARCHAR(128))");
        assertLoad(0, "AL,Jackson","MA");
        assertThat(errorStream.toString(),
                   containsString("No value specified for parameter 2."));
    }

    @Test
    public void testRetry() throws Exception {
        loadDDL("DROP TABLE IF EXISTS states",
                "CREATE TABLE states(abbrev CHAR(4) PRIMARY KEY, name VARCHAR(128))");
        options.maxRetries = 5;
        String[] rows = new String[100];
        List<List<Object>> expected = new ArrayList<>();
        for (int i=0; i<100; i++) {
            rows[i] = String.format("A%03d,named%d",i,i);
            expected.add(Arrays.asList((Object) String.format("A%03d", i), "named" + i));
        }
        DdlRunner ddlRunner = new DdlRunner();
        Thread ddlThread = new Thread(ddlRunner);
        ddlThread.start();
        try {
            assertLoad(100, rows);
        } finally {
            ddlRunner.keepGoing = false;
            ddlThread.stop();
        }
        // just to make sure the ddlThread is done.
        Thread.sleep(4);
        checkQuery("SELECT * FROM states ORDER BY abbrev", expected);
    }

    @Test
    public void testBigInt() throws Exception {
        testDataType("BIGINT", Arrays.asList("-9223372036854775808", "-1", "0", "1", "9223372036854775807"),
                     -9223372036854775808L, -1L, 0L, 1L, 9223372036854775807L);
    }

    @Test
    public void testBlob() throws Exception {
        // TODO figure out real binary encoding story
        testDataType("BLOB", Arrays.asList("\000\003"),
                     new byte[] {0,3});
    }

    @Test
    public void testBoolean() throws Exception {
        testDataType("BOOLEAN", Arrays.asList("true", "TRUE", "false", "FALSE"),
                     true, true, false, false);
    }

    @Test
    public void testChar() throws Exception {
        testDataType("CHAR", Arrays.asList("a", "X", "9"), "a", "X", "9");
    }

    @Test
    public void testChar20() throws Exception {
        testDataType("CHAR(3)", Arrays.asList("abx", "230"), "abx", "230");
    }

    /**
     * COPY T to 'x.csv' WITH (FORMAT CSV) outputs like:
     *   a,\141,a
     *   0,\000,0
     * or (with CHAR(3) FOR BIT DATA) -- the last one is just "a"
     *   ",",\054\054\054,","
     *   0,\000\000\000,0
     *   a,\141\000\000,a
     *
     * our current loader supports that.
     **/
    @Test
    public void testCharForBitData() throws Exception {
        // Note these are escape sequences, so \120 is the character: 'P'
        // our COPY command outputs the string "\120" (4 characters)g
        // TODO figure out real binary encoding story
        testDataType("CHAR FOR BIT DATA", Arrays.asList("\000", "\120", "\177"),
                     new byte[] {0}, new byte[] {0120}, new byte[] {0177});
    }

    @Test
    public void testChar5ForBitData() throws Exception {
        // Note these are escape sequences, so \120 is the character: 'P'
        // our COPY command outputs the string "\120" (4 characters)
        // TODO figure out real binary encoding story
        testDataType("CHAR(5) FOR BIT DATA", Arrays.asList("\120\000\030\047\133"),
                     new byte[] {0120, 0, 030, 047, 0133} );
    }

    @Test
    public void testClob() throws Exception {
        List<String> strings = Arrays.asList("Here is my first large string", "Lorem ipsum dolor sit amet consectetur adipiscing elit. " +
                "Donec a diam lectus. Sed sit amet ipsum mauris. " +
                "Maecenas congue ligula ac quam viverra nec consectetur ante hendrerit.");
        testDataType("CLOB", strings, strings.toArray());
    }

    @Test
    public void testDate() throws Exception {
        testDataType("DATE", Arrays.asList("1970-01-30", "2003-11-27", "2024-08-28"),
                     date(1970,1,30), date(2003,11,27), date(2024,8,28));
    }

    @Test
    public void testDateTime() throws Exception {
        testDataType("DATETIME", Arrays.asList("1970-01-30 03:24:56", "2003-11-27 23:04:16", "2024-08-28 12:59:05"),
                     timestamp(1970,1,30,3,24,56), timestamp(2003,11,27,23,4,16), timestamp(2024,8,28,12,59,05));
    }

    @Test
    public void testDecimal() throws Exception {
        testDataType("DECIMAL(10,2)", Arrays.asList("12345678.94", "333.33", "-407.74"),
                     new BigDecimal("12345678.94"), new BigDecimal("333.33"), new BigDecimal("-407.74"));
    }

    @Test
    public void testDouble() throws Exception {
        testDataType("DOUBLE", Arrays.asList("34.29", "1.345E240", "-408.9", "9.42E-293"),
                     34.29D, 1.345E240, -408.9, 9.42E-293);
    }

    @Test
    public void testGuid() throws Exception {
        testDataType("GUID", Arrays.asList("64e79dec-ce47-4e06-85da-66a594786c6b", "d7c73255-b30d-4084-a8bd-b66b05b7e402"),
                     UUID.fromString("64e79dec-ce47-4e06-85da-66a594786c6b"), UUID.fromString("d7c73255-b30d-4084-a8bd-b66b05b7e402"));
    }

    @Test
    public void testXBadGuid() throws Exception {
        testBadDataType("GUID", Arrays.asList("64e79dec-ce47-4e06-85da", "xxxxxxxx-b30d-4084-a8bd-b66b05b7e402"));
        assertThat(errorStream.toString(),
                   containsString("Invalid UUID string: 64e79dec-ce47-4e06-85da"));
    }

    @Test
    public void testInt() throws Exception {
        testDataType("INT", Arrays.asList("34", "-506"),
                     34, -506);
    }

    @Test
    public void testReal() throws Exception {
        testDataType("REAL", Arrays.asList("22.978", "2.9483E24", "-408.9", "1.5E-33"),
                     22.978F, 2.9483E24F, -408.9F, 1.5E-33F);
    }

    @Test
    public void testTime() throws Exception {
        testDataType("TIME", Arrays.asList("04:25:48", "11:08:22", "23:58:09"),
                     time(4,25,48), time(11,8,22), time(23,58,9));
    }

    @Test
    public void testVarchar10() throws Exception {
        List<String> strings = Arrays.asList("123456789", "bobby");
        testDataType("VARCHAR(10)", strings, strings.toArray());
    }

    @Test
    public void testVarchar10ForBitData() throws Exception {
        // Note these are escape sequences, so \120 is the character: 'P'
        // our COPY command outputs the string "\120" (4 characters)
        // TODO figure out real binary encoding story
        //        testDataType("VARCHAR(10) FOR BIT DATA");
    }

    @Test
    public void testSplit() throws Exception {
        // Note: right now we need the newlines because of a bug in LineReader.
        // Since the mysql dumps always put newlines, hold off on fixing it until
        // LineReader is removed.
        options.nthreads = 2;
        loadDDL("DROP TABLE IF EXISTS states",
                "CREATE TABLE states (abbrev CHAR(2) PRIMARY KEY, name VARCHAR(128))");
        assertLoad(7, "a,b", "c,d", "e,f",
                   "\"Bo\",\"Suzie\"", "\"Al\",\"Jen\"",
                   "x,y","u,v");
        checkQuery("SELECT * FROM states ORDER BY abbrev", Arrays.asList(listO("Al", "Jen"), listO("Bo", "Suzie"), listO("a", "b"), listO("c", "d"), listO("e", "f"), listO("u", "v"), listO("x", "y")));
    }

    private <T> void testBadDataType(String dataType, List<String> inputs) throws Exception
    {
        expectsErrorOutput = true;
        loadDDL("DROP TABLE IF EXISTS states",
                "CREATE TABLE states(key CHAR(4) PRIMARY KEY, value " + dataType + ")");
        for (String input : inputs) {
            assertLoad(0, String.format("A000,%s",input));
            checkQuery("SELECT * FROM states ORDER BY key", new ArrayList<List<Object>>());
        }
    }

    private <T> void testDataType(String dataType, List<String> inputs, T... values) throws Exception
    {
        loadDDL("DROP TABLE IF EXISTS states",
                "CREATE TABLE states(key CHAR(4) PRIMARY KEY, value " + dataType + ")");
        assertEquals("Invalidly written test", inputs.size(), values.length);
        String[] rows = new String[inputs.size()];
        List<List<Object>> expected = new ArrayList<>();
        for (int i=0; i<inputs.size(); i++) {
            rows[i] = String.format("A%03d,%s",i,inputs.get(i));
            expected.add(Arrays.asList((Object) String.format("A%03d", i), values[i]));
        }
        assertLoad(values.length, rows);
        checkQuery("SELECT * FROM states ORDER BY key", expected);
    }

}
