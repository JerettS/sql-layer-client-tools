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
import java.util.ArrayList;
import java.util.List;

import static com.foundationdb.sql.client.load.LineReaderCsvBufferTest.list;
import static org.junit.Assert.assertArrayEquals;
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
        checkQuery("SELECT * FROM states", list(listO(1, 348)));
    }

    @Test
    public void testSingleQuotedFieldFunkyStuff2() throws Exception {
        loadDDL("DROP TABLE IF EXISTS states",
                "CREATE TABLE states(abbrev CHAR(2), name VARCHAR(30), abbrev2 char(2))");
        assertLoad(1, "INSERT INTO states VALUES ('AB', 'boo is \\0 \\b \\n \\r \\t \\Z cool', \"XY\");");
        checkQuery("SELECT * FROM states", list(listO("AB", "boo is \u0000 \b \n \r \t \u001A cool", "XY")));
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
            ddlThread.stop();
        }
        checkQuery("SELECT * FROM states ORDER BY abbrev", expected);
    }

    @Test
    public void testMultipleRows() throws Exception {
        loadDDL("DROP TABLE IF EXISTS states",
                "CREATE TABLE states (abbrev CHAR(2) PRIMARY KEY, name VARCHAR(128))");
        assertLoad(2, "INSERT INTO `states` VALUES (AL,Birmingham);",
                   "INSERT INTO `states` VALUES (MA,Boston);");
        checkQuery("SELECT * FROM states", list(listO("AL","Birmingham"), listO("MA","Boston")));
    }


    @Test
    public void testMultipleRowRows() throws Exception {
        loadDDL("DROP TABLE IF EXISTS states",
                "CREATE TABLE states (abbrev CHAR(2) PRIMARY KEY, name VARCHAR(128))");
        assertLoad(4, "INSERT INTO `states` VALUES (AL,Birmingham),(TX, Austin);",
                   "INSERT INTO `states` VALUES (MA,Boston),(AK,Denali);");
        checkQuery("SELECT * FROM states", list(listO("AK", "Denali"), listO("AL","Birmingham"),
                                                listO("MA","Boston"), listO("TX", "Austin")));
    }

    // TODO schema in table name? what does mysql do when you dump multiple schemas

    @Test
    public void testEscapeTableName() throws Exception {
        String escapedTable = "\"the ; , \"\" bad ; , ? ? states\"";
        String mysqlTable = "`the ; , \" bad ; , ? ? states`";
        loadDDL("DROP TABLE IF EXISTS " + escapedTable,
                "CREATE TABLE " + escapedTable + " (abbrev CHAR(2) PRIMARY KEY, name VARCHAR(128))");
        assertLoad(2, "INSERT INTO " + mysqlTable + " VALUES (AL,Birmingham);",
                   "INSERT INTO " + mysqlTable + " VALUES (MA,Boston);");
        checkQuery("SELECT * FROM " + escapedTable, list(listO("AL","Birmingham"), listO("MA", "Boston")));
    }

    @Test
    public void testLowercaseInsert() throws Exception {
        loadDDL("DROP TABLE IF EXISTS states",
                "CREATE TABLE states(x int PRIMARY KEY, y int)");
        assertLoad(1, "insert into `states` values (1, 348);");
        checkQuery("SELECT * FROM states", list(listO(1, 348)));
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

    // TODO test data types
    // tinyInt______________3____________________3
    // smallInt_____________-385_________________-385
    // mediumInt____________84935________________84935
    // int__________________-47483648____________-47483648
    @Test
    public void testInt() throws Exception {
        testDataType("INT", list("-47483648", "40283"),
                     -47483648, 40283);
    }

    // bigInt_______________-9223372036854775803_-9223372036854775803
    @Test
    public void testBigInt() throws Exception {
        testDataType("BIGINT", list("-9223372036854775803"),
                     -9223372036854775803L);
    }

    // decimal______________958.97_______________958.97
    @Test
    public void testDecimal() throws Exception {
        testDataType("DECIMAL(5,2)", list("958.97"), new BigDecimal("958.97"));
    }

    // numeric______________2983749.391__________2983749.391
    @Test
    public void testNumeric() throws Exception {
        testDataType("DECIMAL(17,3)", list("2983749.391"), new BigDecimal("2983749.391"));
    }
    // float________________230987_______________230987.293
    // double_______________2.039e28_____________2.039E28
    @Test
    public void testDouble() throws Exception {
        testDataType("DOUBLE", list("2.039e28"),
                     2.039E28);
    }
    // bit__________________')'__________________b'101001'
    // date_________________'1000-02-03'_________'1000-02-03'
    @Test
    public void testDate() throws Exception {
        testDataType("DATE", list("'1000-02-03'"),
                     date(1000,2,3));
    }
    // datetime_____________'2045-04-17 08:43:56'_'2045-04-17 08:43:56'
    // timestamp____________'1970-04-17 13:43:56'_'1970-04-17 08:43:56'
    @Test
    public void testDatetime() throws Exception {
        testDataType("DATETIME", list("'2045-04-17 08:43:56'","'1970-04-17 13:43:56'"),
                     timestamp(2045, 04, 17, 8, 43, 56), timestamp(1970, 04, 17, 13, 43, 56));
    }
    // don't worry mysql doesn't dump the microseconds if it has them.
    // time_________________'-15:22:58'__________'-15:22:58'
    // timeWithFractionalSeconds_'483:08:27'__________'483:08:27.493028'
    @Test
    public void testTime() throws Exception {
        testDataType("TIME", list("'-15:22:58'"),
                     time(-15,22,58));
    }
    // NOTE there's nothing we can really do about years right now, they're printed out weirdly
    // e.g. YEAR(2) for 2006 prints out as 36. Year(4) makes sense, it prints out as the integer for the year.

    // char_________________'here'_______________'here'
    // varchar______________'over there'_________'over there'
    @Test
    public void testVarchar() throws Exception {
        testDataType("VARCHAR(17)", list("over there"),
                     "over there");
    }
    // binary_______________'4Uc               '_X'12345563'
    @Test
    public void testBinary() throws Exception {
        // in mysql this would be BINARY(19)
        testDataType("CHAR(19) FOR BIT DATA", list("'\004\037\123\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000'"),
                     bytes(04,037,0123,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0));
    }
    // varbinary____________'#??#	'______________X'012394852309'
    // blob_________________' 8I?B0??	??T????'___X'2038491089423095980981039854af98b23908efc289'
    @Test
    public void testVarbinary() throws Exception {
        // in mysql this would be varbinary(19) or blob
        testDataType("VARCHAR(19) FOR BIT DATA", list("'\\''","'\\\"'","'b\000\007\234\037'","'\u4379'"),
                     bytes(0x27), bytes(0x22), bytes(0x62,0x0,0x7,0234,037),bytes(0x47,0x79));
    }
    // text_________________'Lorem ipsum dolor sit amet'_'Lorem ipsum dolor sit amet'
    // enum_________________'SECOND'_____________'second'
    // set__________________'A|B'________________'a,b'
    // tinyintAsBool________1____________________1
    // intZerofill__________00017________________17
    @Test
    public void testIntZerofill() throws Exception {
        // in mysql this would be INT(5) ZEROFILL
        testDataType("INT", list("00017"), 17);
    }

    private <T> void testDataType(String dataType, List<String> inputs, T... values) throws Exception
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
