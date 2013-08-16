/**
 * Copyright (C) 2012 Akiban Technologies Inc.
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

    @Parameters
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
        Statement stmt = conn.createStatement();
        for (String sql : loaded.split("\\;\\s*")) {
            stmt.execute(sql);
        }
        stmt.close();
        conn.close();

        File dumpFile = File.createTempFile("dump-", ".sql");
        DumpClient client = new DumpClient();
        client.setOutputFile(dumpFile);
        client.addSchema(SCHEMA_NAME);
        client.dump();
        
        String dumped = fileContents(dumpFile);

        assertEquals(caseName, loaded, dumped);
    }

}
