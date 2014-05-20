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

package com.foundationdb.sql.client.dump;

import com.foundationdb.sql.client.ClientTestBase;

/* (Not separate from server yet.)
import com.foundationdb.junit.NamedParameterizedRunner;
import com.foundationdb.junit.NamedParameterizedRunner.TestParameters;
import com.foundationdb.junit.Parameterization;
*/
import com.foundationdb.sql.client.StatementHelper;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized.Parameters;
import org.junit.runners.Parameterized;
import static org.junit.Assert.*;

import java.io.*;
import java.sql.*;
import java.util.*;

@RunWith(Parameterized.class)
public class DumpClientTest extends ClientTestBase
{
    public static final File RESOURCE_DIR =
        new File("src/test/resources/"
                 + DumpClientTest.class.getPackage().getName().replace('.', '/'));
    public static final String SQL_PATTERN = ".*\\.sql";

    @Parameters(name="{0}")
    public static Collection<Object[]> dumps() throws Exception {
        List<Object[]> result = new ArrayList<Object[]>();
        for (File sqlFile : listMatchingFiles(RESOURCE_DIR, SQL_PATTERN)) {
            String caseName = sqlFile.getName().replace(".sql", "");
            result.add(new Object[] { caseName, sqlFile });
        }
        return result;
    }

    private String caseName;
    private File loadFile;

    public DumpClientTest(String caseName, File loadFile) {
        this.caseName = caseName;
        this.loadFile = loadFile;
    }

    @Before
    @After
    public void cleanUp() throws Exception {
        dropSchema();
    }

    @Test
    public void testLoadDump() throws Exception {
        // Take file from previous run, load it and dump again and
        // ensure it's the same.
        String loaded = fileContents(loadFile);

        Connection conn = openConnection();
        StatementHelper helper = new StatementHelper(conn);
        for (String sql : loaded.split("\\;\\s*")) {
            try {
                helper.execute(sql);
            }
            catch (SQLException ex) {
                if (sql.indexOf("IGNORE ERRORS") < 0)
                    throw ex;
                else
                    System.out.println("IGNORED: " + ex);
            }
        }
        helper.close();
        conn.close();

        File dumpFile = File.createTempFile("dump-", ".sql");
        dumpFile.deleteOnExit();

        DumpClientOptions options = new DumpClientOptions();
        fillBaseOptions(options);
        options.outputFile = dumpFile;
        options.schemas.add(SCHEMA_NAME);

        DumpClient client = new DumpClient(options);
        client.dump();
        
        String dumped = fileContents(dumpFile);

        assertEquals(caseName, loaded, dumped);
    }

}
