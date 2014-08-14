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

import org.junit.Test;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.hamcrest.collection.IsArrayWithSize.arrayWithSize;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.hamcrest.collection.IsArrayContainingInOrder.arrayContaining;
import static org.hamcrest.CoreMatchers.containsString;

public class MySQLLoaderTest extends LoaderTestBase
{

    @Test
    public void testBasicLoad() throws Exception {
        loadDDL("DROP TABLE IF EXISTS states",
                "CREATE TABLE states(x int PRIMARY KEY, y int)");
        assertLoad(1, "INSERT INTO `states` VALUES (1, 348);");
        checkQuery("SELECT * FROM states", Arrays.asList(listO(1, 348)));
    }

    @Test
    public void testSingleQuotedFieldFunkyStuff2() throws Exception {
        loadDDL("DROP TABLE IF EXISTS states",
                "CREATE TABLE states(abbrev CHAR(2), name VARCHAR(30), abbrev2 char(2))");
        assertLoad(1, "INSERT INTO states VALUES ('AB', 'boo is \\0 \\b \\n \\r \\t \\Z cool', \"XY\");");
        checkQuery("SELECT * FROM states", Arrays.asList(listO("AB", "boo is \u0000 \b \n \r \t \u001A cool", "XY")));
    }

    @Test
    public void testRetry() throws Exception {
        loadDDL("DROP TABLE IF EXISTS states",
                "CREATE TABLE states(abbrev CHAR(4) PRIMARY KEY, name VARCHAR(128))");
        options.maxRetries = 5;
        String[] rows = new String[100];
        List<List<Object>> expected = new ArrayList<>();
        for (int i=0; i<100; i++) {
            rows[i] = String.format("INSERT INTO `states` VALUES (A%03d,named%d);",i,i);
            expected.add(listO(String.format("A%03d", i), "named" + i));
        }
        DdlRunner ddlRunner = new DdlRunner();
        Thread ddlThread = new Thread(ddlRunner);
        ddlThread.start();
        try {
            assertLoad(100, rows);
        } finally {
            ddlRunner.keepGoing = false;
        }
        ddlThread.join();
        checkQuery("SELECT * FROM states ORDER BY abbrev", expected);
    }

    @Test
    public void testMultipleRows() throws Exception {
        loadDDL("DROP TABLE IF EXISTS states",
                "CREATE TABLE states (abbrev CHAR(2) PRIMARY KEY, name VARCHAR(128))");
        assertLoad(2, "INSERT INTO `states` VALUES (AL,Birmingham);",
                   "INSERT INTO `states` VALUES (MA,Boston);");
        checkQuery("SELECT * FROM states", Arrays.asList(listO("AL", "Birmingham"), listO("MA", "Boston")));
    }


    @Test
    public void testMultipleRowRows() throws Exception {
        loadDDL("DROP TABLE IF EXISTS states",
                "CREATE TABLE states (abbrev CHAR(2) PRIMARY KEY, name VARCHAR(128))");
        assertLoad(4, "INSERT INTO `states` VALUES (AL,Birmingham),(TX, Austin);",
                   "INSERT INTO `states` VALUES (MA,Boston),(AK,Denali);");
        checkQuery("SELECT * FROM states", Arrays.asList(listO("AK", "Denali"), listO("AL", "Birmingham"), listO("MA", "Boston"), listO("TX", "Austin")));
    }

    // NOTE: mysqldump does not include the schema, so we don't need to worry about that.
    // If you do dump multiple schemas it uses "USE dbname;". Unless you include the -n
    // option it will also include "CREATE DATABASE ..." lines. We don't handle either of
    // these.

    @Test
    public void testEscapeTableName() throws Exception {
        String escapedTable = "\"the ; , \"\" bad ; , ? ? states\"";
        String mysqlTable = "`the ; , \" bad ; , ? ? states`";
        loadDDL("DROP TABLE IF EXISTS " + escapedTable,
                "CREATE TABLE " + escapedTable + " (abbrev CHAR(2) PRIMARY KEY, name VARCHAR(128))");
        assertLoad(2, "INSERT INTO " + mysqlTable + " VALUES (AL,Birmingham);",
                   "INSERT INTO " + mysqlTable + " VALUES (MA,Boston);");
        checkQuery("SELECT * FROM " + escapedTable, Arrays.asList(listO("AL", "Birmingham"), listO("MA", "Boston")));
    }

    @Test
    public void testLowercaseInsert() throws Exception {
        loadDDL("DROP TABLE IF EXISTS states",
                "CREATE TABLE states(x int PRIMARY KEY, y int)");
        assertLoad(1, "insert into `states` values (1, 348);");
        checkQuery("SELECT * FROM states", Arrays.asList(listO(1, 348)));
    }

    @Test
    public void testIntoOption() throws Exception {
        loadDDL("DROP TABLE IF EXISTS states",
                "CREATE TABLE states(x int PRIMARY KEY, y int)");
        options.target = "not the right table";
        expectsErrorOutput = true;
        assertLoad(-1, "insert into `states` values (1, 348);");
        assertThat(errorStream.toString(), containsString("MySQL import does not support the --into option"));
    }

    @Test
    public void testInt() throws Exception {
        testDataType("INT", Arrays.asList("-47483648", "40283"),
                     -47483648, 40283);
    }

    @Test
    public void testBigInt() throws Exception {
        testDataType("BIGINT", Arrays.asList("-9223372036854775803"),
                     -9223372036854775803L);
    }

    @Test
    public void testDecimal() throws Exception {
        testDataType("DECIMAL(5,2)", Arrays.asList("958.97"), new BigDecimal("958.97"));
    }

    @Test
    public void testNumeric() throws Exception {
        testDataType("DECIMAL(17,3)", Arrays.asList("2983749.391"), new BigDecimal("2983749.391"));
    }

    @Test
    public void testDouble() throws Exception {
        testDataType("DOUBLE", Arrays.asList("2.039e28"),
                     2.039E28);
    }

    @Test
    public void testDate() throws Exception {
        testDataType("DATE", Arrays.asList("'1000-02-03'"),
                     date(1000,2,3));
    }

    @Test
    public void testDatetime() throws Exception {
        testDataType("DATETIME", Arrays.asList("'2045-04-17 08:43:56'", "'1970-04-17 13:43:56'"),
                     timestamp(2045, 4, 17, 8, 43, 56), timestamp(1970, 4, 17, 13, 43, 56));
    }

    // don't worry mysql doesn't dump the microseconds if it has them.
    @Test
    public void testTime() throws Exception {
        testDataType("TIME", Arrays.asList("'15:22:58'"),
                     time(15, 22, 58));
    }

    @Test
    public void testNegativeTime() throws Exception {
        // Note: I inlined testDataTypes and checkQuery here so that I could change it to call
        // getString instead of getObject because jdbc doesn't support negative times
        // since we are also, most likely, not going to support negative times soon, I'm leaving
        // this as a mess with duplicated code.
        List<String> inputs = Arrays.asList("'-15:22:58'");
        // So, just a little tidbit of information, time(-15, 22, 58) becomes 9:22:58
        String[] values = new String[] {"-15:22:58"};
        loadDDL("DROP TABLE IF EXISTS states",
                "CREATE TABLE states(key CHAR(4) PRIMARY KEY, value " + "TIME" + ")");
        assertEquals("Invalidly written test", inputs.size(), values.length);
        String[] rows = new String[inputs.size()];
        List<List<Object>> expected = new ArrayList<>();
        for (int i=0; i<inputs.size(); i++) {
            rows[i] = String.format("INSERT INTO states VALUES (A%03d,%s);",i,inputs.get(i));
            expected.add(listO(String.format("A%03d",i), values[i]));
        }
        assertLoad(values.length, rows);
        Connection conn = openConnection();
        Statement stmt = conn.createStatement();
        ResultSet rs = stmt.executeQuery("SELECT * FROM states ORDER BY key");
        int ncols = rs.getMetaData().getColumnCount();
        List<List<Object>> actual = new ArrayList<>();
        while (rs.next()) {
            List<Object> row = new ArrayList<>();
            for (int i = 1; i <= ncols; i++) {
                // difference with testDataTypes
                row.add(rs.getString(i));
            }
            actual.add(row);
        }
        rs.close();
        stmt.close();
        conn.close();

        Object[][] expectedArray = new Object[expected.size()][];
        for (int i=0; i< expected.size(); i++) {
            expectedArray[i] = expected.get(i).toArray();
        }

        Object[][] actualArray = new Object[actual.size()][];
        for (int i=0; i<actual.size(); i++) {
            actualArray[i] = actual.get(i).toArray();
        }
        if (expectedArray.length > 0) {
            assertThat("SELECT * FROM states ORDER BY key",actualArray, arrayContaining(expectedArray));
        } else {
            assertThat("SELECT * FROM states ORDER BY key", actualArray, arrayWithSize(0));
        }
    }

    // NOTE there's nothing we can really do about years right now, they're printed out weirdly
    // e.g. YEAR(2) for 2006 prints out as 36. Year(4) makes sense, it prints out as the integer for the year.

    @Test
    public void testVarchar() throws Exception {
        testDataType("VARCHAR(17)", Arrays.asList("over there"),
                     "over there");
    }

    @Test
    public void testBinary() throws Exception {
        // in mysql this would be BINARY(19)
        // NOTE: \nnn is octal
        testDataType("CHAR(19) FOR BIT DATA", Arrays.asList("'\004\037\123\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000'"),
                     bytes(04,037,0123,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0));
    }

    @Test
    public void testVarbinary() throws Exception {
        // in mysql this would be varbinary(19) or blob
        // NOTE: \nnn is octal
        // TODO figure out real binary data story, so that you can get those high bytes (e.g. \234)
        testDataType("VARCHAR(19) FOR BIT DATA", Arrays.asList("'\\''", "'\\\"'", "'b\000\007\037'", "'\077\024'"),
                     bytes(0x27), bytes(0x22), bytes(0x62,0x0,0x7,037),bytes(077,024));
    }

    @Test
    public void testIntZerofill() throws Exception {
        // in mysql this would be INT(5) ZEROFILL
        testDataType("INT", Arrays.asList("00017"), 17);
    }

    @Test
    public void testSplit() throws Exception {
        // Note: right now we need the newlines because of a bug in LineReader.
        // Since the mysql dumps always put newlines, hold off on fixing it until
        // LineReader is removed.
        String line1 = "INSERT INTO states VALUES (a,b),\n\t\t(c,d),\n\t\t(e,f);\n";
        String line2 = "INSERT INTO states VALUES (\"Bo\",\"Suzie\"),(\"Al\",\"Jen\");\n";
        String line3 = "INSERT INTO states VALUES (x,y),\n\t\t(u,v);\n";
        options.nthreads = 2;
        loadDDL("DROP TABLE IF EXISTS states",
                "CREATE TABLE states (abbrev CHAR(2) PRIMARY KEY, name VARCHAR(128))");
        assertLoad(7, line1, line2, line3);
        checkQuery("SELECT * FROM states ORDER BY abbrev", Arrays.asList(listO("Al", "Jen"), listO("Bo", "Suzie"), listO("a", "b"), listO("c", "d"), listO("e", "f"), listO("u", "v"), listO("x", "y")));
    }


    private void testDataType(String dataType, List<String> inputs, Object... values) throws Exception
    {
        loadDDL("DROP TABLE IF EXISTS states",
                "CREATE TABLE states(key CHAR(4) PRIMARY KEY, value " + dataType + ")");
        assertEquals("Invalidly written test", inputs.size(), values.length);
        String[] rows = new String[inputs.size()];
        List<List<Object>> expected = new ArrayList<>();
        for (int i=0; i<inputs.size(); i++) {
            rows[i] = String.format("INSERT INTO states VALUES (A%03d,%s);",i,inputs.get(i));
            expected.add(listO(String.format("A%03d",i), values[i]));
        }
        assertLoad(values.length, rows);
        checkQuery("SELECT * FROM states ORDER BY key", expected);
    }

}
