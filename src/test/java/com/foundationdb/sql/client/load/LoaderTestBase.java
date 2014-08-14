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

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.sql.Connection;
import java.sql.Date;
import java.sql.ResultSet;
import java.sql.Statement;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.GregorianCalendar;
import java.util.List;
import java.util.Locale;
import java.util.TimeZone;

import static com.foundationdb.sql.client.load.LineReaderCsvBufferTest.tmpFileFrom;
import static org.hamcrest.collection.IsArrayContainingInOrder.arrayContaining;
import static org.hamcrest.collection.IsArrayWithSize.arrayWithSize;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

public class LoaderTestBase extends ClientTestBase{

    protected LoadClientOptions options;
    protected ByteArrayOutputStream errorStream;
    protected boolean expectsErrorOutput = false;
    // TODO if there's an error it just logs to StandardError, and doesn't insert
    // for the most part. Since all error handling will be redone shortly, this is
    // the stopgap, so that tests with errors don't flood the output when you run
    // mvn test
    private PrintStream originalError;

    protected static Time time(int hour, int minute, int second) {
        return new Time(date(1970,1,1,hour,minute,second).getTime());
    }

    protected static Date date(int year, int month, int day) {
        return new Date(date(year, month, day, 0, 0, 0).getTime());
    }

    protected static Timestamp timestamp(int year, int month, int day, int hour, int minute, int second) {
        return new Timestamp(date(year,month,day,hour,minute,second).getTime());
    }

    private static java.util.Date date(int year, int month, int day, int hour, int minute, int second) {
        GregorianCalendar calendar = new GregorianCalendar(TimeZone.getDefault(), Locale.US);
        calendar.set(year, month-1, day, hour, minute, second);
        calendar.set(Calendar.MILLISECOND, 0);
        return calendar.getTime();
    }

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
    }

    protected byte[] bytes(int... bs) {
        byte[] bytes = new byte[bs.length];
        for (int i=0; i<bs.length; i++) {
            bytes[i] = (byte)bs[i];
        }
        return bytes;
    }

    protected List<Object> listO(Object... objects) {
        return Arrays.asList(objects);
    }

    protected void assertLoad(int expectedCount, String... rows) throws Exception {
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
        if (expectedArray.length > 0) {
            assertThat(query,actualArray, arrayContaining(expectedArray));
        } else {
            assertThat(query, actualArray, arrayWithSize(0));
        }
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


    protected class DdlRunner implements Runnable {
        public boolean keepGoing = true;
        @Override
        public void run() {
            int timeSlept = 0;
            try {
                while (keepGoing) {
                    // The goal is to get about 3 retries for the csv and mysql tests
                    // Sleep 1ms at a time so that we can set keepGoing to false, and it will stop
                    // within a millisecond, instead of sleeping for the next minute
                    if (timeSlept == 5 || timeSlept == 18 || timeSlept == 256 || timeSlept == 1000) {
                        loadDDL("DROP TABLE IF EXISTS foo", "CREATE TABLE foo (abbrev CHAR(4))");
                    }
                    Thread.sleep(1);
                    timeSlept++;
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }
}
