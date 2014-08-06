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

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import static com.foundationdb.sql.client.load.LineReaderCsvBufferTest.list;
import static com.foundationdb.sql.client.load.LineReaderCsvBufferTest.tmpFileFrom;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertArrayEquals;

public class CsvLoaderTest extends ClientTestBase
{
    private LoadClientOptions options;

    @Before
    @After
    public void cleanUp() throws Exception {
        dropSchema();
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

    // TODO     @Test
    public void testTooManyColumns() throws Exception {assertEquals("implemented", "NOT");}

    // TODO    @Test
    public void testTooFewColumns() throws Exception {assertEquals("implemented", "NOT");}

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
        testDataType("CHAR FOR BIT DATA", list("\000", "\120", "\310"),
                     new byte[] {0}, new byte[] {80}, new byte[] {-56});
    }

    // @Test
    // public void testChar5ForBitData() throws Exception {
    //     testDataType("CHAR5FORBITDATA");
    // }

    // @Test
    // public void testClob() throws Exception {
    //     testDataType("CLOB");
    // }

    // @Test
    // public void testDate() throws Exception {
    //     testDataType("DATE");
    // }

    // @Test
    // public void testDateTime() throws Exception {
    //     testDataType("DATETIME");
    // }

    // @Test
    // public void testDecimal() throws Exception {
    //     testDataType("DECIMAL");
    // }

    // @Test
    // public void testDouble() throws Exception {
    //     testDataType("DOUBLE");
    // }

    // @Test
    // public void testGuid() throws Exception {
    //     testDataType("GUID");
    // }

    // @Test
    // public void testInt() throws Exception {
    //     testDataType("INT");
    // }

    // @Test
    // public void testReal() throws Exception {
    //     testDataType("REAL");
    // }

    // @Test
    // public void testTime() throws Exception {
    //     testDataType("TIME");
    // }

    // @Test
    // public void testVarchar10() throws Exception {
    //     testDataType("VARCHAR10");
    // }

    // @Test
    // public void testVarchar10ForBitData() throws Exception {
    //     testDataType("VARCHAR10FORBITDATA");
    // }

    // @Test
    // public void test() throws Exception {
    //     testDataType();
    // }

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

    private class DdlRunner implements Runnable {
        public boolean keepGoing = true;
        @Override
        public void run() {
            int sleepTime = 1;
            try {
                while (keepGoing) {
                    loadDDL("DROP TABLE IF EXISTS foo", "CREATE TABLE foo (abbrev CHAR(4))");
                    Thread.sleep(sleepTime);
                    sleepTime *= 10;
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }
}
