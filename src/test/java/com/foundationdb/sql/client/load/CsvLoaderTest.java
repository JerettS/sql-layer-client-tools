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

import com.foundationdb.sql.client.ClientTestBase;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import org.postgresql.util.PSQLException;

import java.io.PrintStream;
import java.io.ByteArrayOutputStream;
import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.Date;
import java.sql.Time;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.GregorianCalendar;
import java.util.Locale;
import java.util.TimeZone;
import java.util.List;
import java.util.UUID;

import static com.foundationdb.sql.client.load.LineReaderCsvBufferTest.list;
import static com.foundationdb.sql.client.load.LineReaderCsvBufferTest.tmpFileFrom;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertArrayEquals;

public class CsvLoaderTest extends ClientTestBase
{
    private LoadClientOptions options;
    // TODO if there's an error it just logs to StandardError, and doesn't insert
    // for the most part. Since all error handling will be redone shortly, this is
    // the stopgap, so that tests with errors don't flood the output when you run
    // mvn test
    private PrintStream originalError;
    private ByteArrayOutputStream errorStream;
    private boolean expectsErrorOutput = false;

    @Before
    @After
    public void cleanUp() throws Exception {
        dropSchema();
    }

    @Before
    public void setStandardError() throws Exception {
        originalError = System.err;
        errorStream = new ByteArrayOutputStream();
        System.setErr(new PrintStream(errorStream));
    }

    @After
    public void resetStandardError() throws Exception {
        System.setErr(originalError);
    }

    @Before
    public void setupOptions() {
        options = new LoadClientOptions();
        fillBaseOptions(options);
        options.schema = SCHEMA_NAME;
        options.quiet = true;
        options.format = Format.CSV;
        options.target = "states";
    }

    @Test
    public void testBasicLoad() throws Exception {
        loadDDL("DROP TABLE IF EXISTS states",
                "CREATE TABLE states(abbrev CHAR(2) PRIMARY KEY, name VARCHAR(128))");
        assertLoad(2, "AL,Birmingham","MA,Boston");
        checkQuery("SELECT * FROM states", list(list((Object) "AL", "Birmingham"), list((Object) "MA", "Boston")));
    }

    @Test
    public void testEscapeTableName() throws Exception {
        String escapedTable = "\"the ; , \"\" bad ; , ? ? states\"";
        loadDDL("DROP TABLE IF EXISTS " + escapedTable,
                "CREATE TABLE " + escapedTable + " (abbrev CHAR(2) PRIMARY KEY, name VARCHAR(128))");
        options.target = "the ; , \" bad ; , ? ? states";
        assertLoad(2, "AL,Birmingham","MA,Boston");
        checkQuery("SELECT * FROM " + escapedTable, list(list((Object)"AL","Birmingham"), list((Object)"MA","Boston")));
    }

    @Test
    public void testEscapeColumnName() throws Exception {
        String escapedColumnName = "\"the ; , \"\" bad ; , ? ? abbreviation\"";
        loadDDL("DROP TABLE IF EXISTS states ",
                "CREATE TABLE states (" + escapedColumnName + " CHAR(2) PRIMARY KEY, name VARCHAR(128))");
        assertLoad(2, "AL,Birmingham","MA,Boston");
        checkQuery("SELECT * FROM states", list(list((Object)"AL","Birmingham"), list((Object)"MA","Boston")));
    }

    @Test
    public void testEscapeColumnNameWithHeader() throws Exception {
        String escapedColumnName = "\"the (; ,) \"\" bad ; , ? ? abbreviation\"";
        loadDDL("DROP TABLE IF EXISTS states ",
                "CREATE TABLE states (" + escapedColumnName + " CHAR(2) PRIMARY KEY, name VARCHAR(128))");
        options.format = Format.CSV_HEADER;
        // conveniently csv & sql escape in the same way for table/column names
        assertLoad(2, escapedColumnName + ",name", "AL,Birmingham","MA,Boston");
        checkQuery("SELECT * FROM states", list(list((Object)"AL","Birmingham"), list((Object)"MA","Boston")));
    }

    @Test
    public void testEmptyCsvWithoutHeader() throws Exception {
        options.format = Format.CSV;
        try {
            assertLoad(0, "");
            assertEquals("An exception to be thrown", "no exception was thrown");
        } catch (IndexOutOfBoundsException e) {
            assertEquals("Csv file is empty", e.getMessage());
        }
    }

    @Test
    public void testEmptyCsvWithHeader() throws Exception {
        options.format = Format.CSV_HEADER;
        try {
            assertLoad(0, "");
            assertEquals("An exception to be thrown", "no exception was thrown");
        } catch (IndexOutOfBoundsException e) {
            assertEquals("Csv file is empty", e.getMessage());
        }
    }

    @Test
    public void testTooManyColumnsInHeader() throws Exception {
        // TODO for now all we can do is assert that no rows are inserted, eventually better
        // error handling will exist and we'll be able to have better tests
        expectsErrorOutput = true;
        loadDDL("DROP TABLE IF EXISTS states ",
                "CREATE TABLE states (abbrev CHAR(2) PRIMARY KEY, name VARCHAR(128))");
        options.format = Format.CSV_HEADER;
        assertLoad(0, "abbrev,name,other", "AL,Birmingham","MA,Boston");
    }

    @Test
    public void testTooManyColumnsWithoutHeader() throws Exception {
        // TODO for now all we can do is assert that no rows are inserted, eventually better
        // error handling will exist and we'll be able to have better tests
        expectsErrorOutput = true;
        loadDDL("DROP TABLE IF EXISTS states ",
                "CREATE TABLE states (abbrev CHAR(2) PRIMARY KEY, name VARCHAR(128))");
        assertLoad(0, "AL,Birmingham,USA","MA,Boston");
    }

    @Test
    public void testTooManyColumnsWithoutHeader2() throws Exception {
        // TODO for now all we can do is assert that no rows are inserted, eventually better
        // error handling will exist and we'll be able to have better tests
        expectsErrorOutput = true;
        loadDDL("DROP TABLE IF EXISTS states ",
                "CREATE TABLE states (abbrev CHAR(2) PRIMARY KEY, name VARCHAR(128))");
        assertLoad(0, "AL,Birmingham","MA,Boston,USA");
    }

    @Test
    public void testTooFewColumnsInHeader() throws Exception {
        // TODO for now all we can do is assert that no rows are inserted, eventually better
        // error handling will exist and we'll be able to have better tests
        expectsErrorOutput = true;
        loadDDL("DROP TABLE IF EXISTS states ",
                "CREATE TABLE states (abbrev CHAR(2) PRIMARY KEY, name VARCHAR(128))");
        options.format = Format.CSV_HEADER;
        assertLoad(0, "abbrev", "AL,Birmingham","MA,Boston");
    }

    @Test
    public void testTooFewColumnsWithoutHeader() throws Exception {
        // TODO for now all we can do is assert that no rows are inserted, eventually better
        // error handling will exist and we'll be able to have better tests
        expectsErrorOutput = true;
        loadDDL("DROP TABLE IF EXISTS states ",
                "CREATE TABLE states (abbrev CHAR(2) PRIMARY KEY, name VARCHAR(128))");
        assertLoad(0, "AL","MA,Boston");
    }

    @Test
    public void testTooFewColumnsWithoutHeader2() throws Exception {
        // TODO for now all we can do is assert that no rows are inserted, eventually better
        // error handling will exist and we'll be able to have better tests
        expectsErrorOutput = true;
        loadDDL("DROP TABLE IF EXISTS states ",
                "CREATE TABLE states (abbrev CHAR(2) PRIMARY KEY, name VARCHAR(128))");
        assertLoad(0, "AL,Jackson","MA");
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
            expected.add(list((Object)String.format("A%03d",i),"named" + i));
        }
        DdlRunner ddlRunner = new DdlRunner();
        Thread ddlThread = new Thread(ddlRunner);
        ddlThread.start();
        assertLoad(100, rows);
        ddlRunner.keepGoing = false;
        ddlThread.stop();
        checkQuery("SELECT * FROM states ORDER BY abbrev", expected);
    }

    @Test
    public void testBigInt() throws Exception {
        testDataType("BIGINT", list("-9223372036854775808", "-1", "0", "1", "9223372036854775807"),
                     -9223372036854775808L, -1L, 0L, 1L, 9223372036854775807L);
    }

    @Test
    public void testBlob() throws Exception {
        // TODO figure out real binary encoding story
        testDataType("BLOB", list("\000\003"),
                     new byte[] {0,3});
    }

    @Test
    public void testBoolean() throws Exception {
        testDataType("BOOLEAN", list("true", "TRUE", "false", "FALSE"),
                     true, true, false, false);
    }

    @Test
    public void testChar() throws Exception {
        testDataType("CHAR", list("a", "X", "9"), "a", "X", "9");
    }

    @Test
    public void testChar20() throws Exception {
        testDataType("CHAR(3)", list("abx", "230"), "abx", "230");
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
        testDataType("CHAR FOR BIT DATA", list("\000", "\120", "\177"),
                     new byte[] {0}, new byte[] {0120}, new byte[] {0177});
    }

    @Test
    public void testChar5ForBitData() throws Exception {
        // Note these are escape sequences, so \120 is the character: 'P'
        // our COPY command outputs the string "\120" (4 characters)
        // TODO figure out real binary encoding story
        testDataType("CHAR(5) FOR BIT DATA", list("\120\000\030\047\133"),
                     new byte[] {0120, 0, 030, 047, 0133} );
    }

    @Test
    public void testClob() throws Exception {
        List<String> strings = list("Here is my first large string",
                                    "Lorem ipsum dolor sit amet consectetur adipiscing elit. " +
                                    "Donec a diam lectus. Sed sit amet ipsum mauris. " +
                                    "Maecenas congue ligula ac quam viverra nec consectetur ante hendrerit.");
        testDataType("CLOB", strings, strings.toArray());
    }

    @Test
    public void testDate() throws Exception {
        testDataType("DATE", list("1970-01-30", "2003-11-27", "2024-08-28"),
                     date(1970,1,30), date(2003,11,27), date(2024,8,28));
    }

    @Test
    public void testDateTime() throws Exception {
        testDataType("DATETIME", list("1970-01-30 03:24:56", "2003-11-27 23:04:16", "2024-08-28 12:59:05"),
                     date(1970,1,30,3,24,56), date(2003,11,27,23,4,16), date(2024,8,28,12,59,05));
    }

    @Test
    public void testDecimal() throws Exception {
        testDataType("DECIMAL(10,2)", list("12345678.94", "333.33", "-407.74"),
                     new BigDecimal("12345678.94"), new BigDecimal("333.33"), new BigDecimal("-407.74"));
    }

    @Test
    public void testDouble() throws Exception {
        testDataType("DOUBLE", list("34.29", "1.345E240", "-408.9", "9.42E-293"),
                     34.29D, 1.345E240, -408.9, 9.42E-293);
    }

    @Test
    public void testGuid() throws Exception {
        testDataType("GUID", list("64e79dec-ce47-4e06-85da-66a594786c6b", "d7c73255-b30d-4084-a8bd-b66b05b7e402"),
                     UUID.fromString("64e79dec-ce47-4e06-85da-66a594786c6b"), UUID.fromString("d7c73255-b30d-4084-a8bd-b66b05b7e402"));
    }

    @Test
    public void testXBadGuid() throws Exception {
        expectsErrorOutput = true;
        testBadDataType("GUID", list("64e79dec-ce47-4e06-85da", "xxxxxxxx-b30d-4084-a8bd-b66b05b7e402"));
    }

    @Test
    public void testInt() throws Exception {
        testDataType("INT", list("34", "-506"),
                     34, -506);
    }

    @Test
    public void testReal() throws Exception {
        testDataType("REAL", list("22.978", "2.9483E24", "-408.9", "1.5E-33"),
                     22.978F, 2.9483E24F, -408.9F, 1.5E-33F);
    }

    @Test
    public void testTime() throws Exception {
        testDataType("TIME", list("04:25:48", "11:08:22", "23:58:09"),
                     time(4,25,48), time(11,8,22), time(23,58,9));
    }

    @Test
    public void testVarchar10() throws Exception {
        List<String> strings = list("123456789", "bobby");
        testDataType("VARCHAR(10)", strings, strings.toArray());
    }

    @Test
    public void testVarchar10ForBitData() throws Exception {
        // Note these are escape sequences, so \120 is the character: 'P'
        // our COPY command outputs the string "\120" (4 characters)
        // TODO figure out real binary encoding story
        //        testDataType("VARCHAR(10) FOR BIT DATA");
    }

    private <T> void testBadDataType(String dataType, List<String> inputs) throws Exception
    {
        // TODO eventually it should do more than just print to stderr, but for now, we'll just have to check that no rows were added
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
            expected.add(list((Object)String.format("A%03d",i),values[i]));
        }
        assertLoad(values.length, rows);
        checkQuery("SELECT * FROM states ORDER BY key", expected);
    }

    private void assertLoad(int count, String... rows) throws Exception {
        LoadClient client = new LoadClient(options);
        try {
            assertEquals(count, client.load(tmpFileFrom(true, rows)));
        }
        finally {
            client.clearConnections();
        }
    }

    protected void checkQuery(String query, List<List<Object>> expected) throws Exception {
        Connection conn = openConnection();
        Statement stmt = conn.createStatement();
        ResultSet rs = stmt.executeQuery(query);
        int ncols = rs.getMetaData().getColumnCount();
        List<List<Object>> actual = new ArrayList<>();
        while (rs.next()) {
            List<Object> row = new ArrayList<>();
            for (int i = 1; i <= ncols; i++) {
                row.add(rs.getObject(i));
            }
            actual.add(row);
        }
        rs.close();
        stmt.close();
        conn.close();

        Object[][] expectedArray = new Object[expected.size()][];
        for (int i=0; i<expected.size(); i++) {
            expectedArray[i] = expected.get(i).toArray();
        }

        Object[][] actualArray = new Object[actual.size()][];
        for (int i=0; i<actual.size(); i++) {
            actualArray[i] = actual.get(i).toArray();
        }
        assertArrayEquals(query, expectedArray, actualArray);
    }

    protected void loadDDL(String... ddl) throws Exception {
        Connection conn = openConnection();
        Statement stmt = conn.createStatement();
        for (String sql : ddl) {
            stmt.execute(sql);
        }
        stmt.close();
        conn.close();
    }

    protected Time time(int hour, int minute, int second) {
        return new Time(date(1970,1,1,hour,minute,second).getTime());
    }

    protected Date date(int year, int month, int day) {
        return date(year, month, day, 0, 0, 0);
    }

    protected Date date(int year, int month, int day, int hour, int minute, int second) {
        GregorianCalendar calendar = new GregorianCalendar(TimeZone.getDefault(), Locale.US);
        calendar.set(year, month-1, day, hour, minute, second);
        calendar.set(Calendar.MILLISECOND, 0);
        return new Date(calendar.getTime().getTime());
    }

    private class DdlRunner implements Runnable {
        public boolean keepGoing = true;
        @Override
        public void run() {
            int sleepTime = 1;
            try {
                while (keepGoing) {
                    loadDDL("DROP TABLE IF EXISTS foo", "CREATE TABLE foo (abbrev CHAR(4))");
                    Thread.sleep(sleepTime);
                    sleepTime *= 2 * sleepTime * sleepTime;
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }
}
