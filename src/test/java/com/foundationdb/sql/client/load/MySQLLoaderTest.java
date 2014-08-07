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
