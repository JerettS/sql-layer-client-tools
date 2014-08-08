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

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.Date;
import java.sql.ResultSet;
import java.sql.Statement;
import java.sql.Time;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.GregorianCalendar;
import java.util.List;
import java.util.Locale;
import java.util.TimeZone;
import java.util.UUID;

import static com.foundationdb.sql.client.load.LineReaderCsvBufferTest.list;
import static com.foundationdb.sql.client.load.LineReaderCsvBufferTest.tmpFileFrom;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

public class MySQLLoaderTest extends ClientTestBase
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
        expectsErrorOutput = false;
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
        options.format = Format.MYSQL_DUMP;
        options.target = "states";
    }

    @Test
    public void testBasicLoad() throws Exception {
        loadDDL("DROP TABLE IF EXISTS states",
                "CREATE TABLE states(x int PRIMARY KEY, y int)");
        assertLoad(1, "INSERT INTO `states` VALUES (1, 348);");
        checkQuery("SELECT * FROM states", list(list((Object) 1, 348)));
    }

    @Test
    public void testSingleQuotedFieldFunkyStuff2() throws Exception {
        loadDDL("DROP TABLE IF EXISTS states",
                "CREATE TABLE states(abbrev CHAR(2), name VARCHAR(30), abbrev2 char(2))");
        assertLoad(1, "INSERT INTO states VALUES ('AB', 'boo is \\0 \\b \\n \\r \\t \\Z cool', \"XY\");");
        checkQuery("SELECT * FROM states", list(list((Object)"AB", "boo is \u0000 \b \n \r \t \u001A cool", "XY")));
    }


    //TODO    @Test
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
        try {
            assertLoad(100, rows);
            ddlRunner.keepGoing = false;
        } finally {
            ddlThread.stop();
        }
        checkQuery("SELECT * FROM states ORDER BY abbrev", expected);
    }

    // TODO test multiple rows

    // TODO schema in table name?

    @Test
    public void testEscapeTableName() throws Exception {
        String escapedTable = "\"the ; , \"\" bad ; , ? ? states\"";
        String mysqlTable = "`the ; , \" bad ; , ? ? states`";
        loadDDL("DROP TABLE IF EXISTS " + escapedTable,
                "CREATE TABLE " + escapedTable + " (abbrev CHAR(2) PRIMARY KEY, name VARCHAR(128))");
        assertLoad(2, "INSERT INTO " + mysqlTable + " VALUES (AL,Birmingham);",
                   "INSERT INTO " + mysqlTable + " VALUES (MA,Boston);");
        checkQuery("SELECT * FROM " + escapedTable, list(list((Object)"AL","Birmingham"), list((Object)"MA","Boston")));
    }

    @Test
    public void testLowercaseInsert() throws Exception {
        loadDDL("DROP TABLE IF EXISTS states",
                "CREATE TABLE states(x int PRIMARY KEY, y int)");
        assertLoad(1, "insert into `states` values (1, 348);");
        checkQuery("SELECT * FROM states", list(list((Object) 1, 348)));
    }

    // TODO options.target thorws exception

    // TODO test data types


    private void assertLoad(int expectedCount, String... rows) throws Exception {
        LoadClient client = new LoadClient(options);
        try {
            long count = client.load(tmpFileFrom(true, rows));
            if (!expectsErrorOutput) {
                assertEquals("Error output stream", "", errorStream.toString());
            }
            assertEquals(expectedCount, count);
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
