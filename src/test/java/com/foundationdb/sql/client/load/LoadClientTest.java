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
public class LoadClientTest extends ClientTestBase
{
    public static final File RESOURCE_DIR =
        new File("src/test/resources/"
                 + LoadClientTest.class.getPackage().getName().replace('.', '/'));
    public static final String PROPERTIES_PATTERN = ".*\\.properties";

    @Parameters(name="{0}")
    public static Collection<Object[]> loads() throws Exception {
        List<Object[]> result = new ArrayList<Object[]>();
        for (File propertiesFile : listMatchingFiles(RESOURCE_DIR, PROPERTIES_PATTERN)) {
            String caseName = propertiesFile.getName().replace(".properties", "");
            result.add(new Object[] { caseName, propertiesFile });
        }
        return result;
    }

    private String caseName;
    private File propertiesFile;

    public LoadClientTest(String caseName, File propertiesFile) {
        this.caseName = caseName;
        this.propertiesFile = propertiesFile;
    }

    @Before
    @After
    public void cleanUp() throws Exception {
        dropSchema();
    }

    @Test
    public void testLoad() throws Exception {
        File dir = propertiesFile.getParentFile();
        Properties properties = new Properties();
        FileInputStream pstr = new FileInputStream(propertiesFile);
        properties.load(pstr);
        pstr.close();

        LoadClientOptions options = new LoadClientOptions();
        fillBaseOptions(options);
        options.schema = SCHEMA_NAME;
        options.quiet = true;

        File ddlFile = null;
        long expectedCount = -1;
        String query = null;
        File expectedFile = null;
        List<File> files = new ArrayList<>();

        for (String key : properties.stringPropertyNames()) {
            String value = properties.getProperty(key);
            if ("format".equals(key))
                options.format = Format.fromName(value);
            else if ("header".equals(key))
                options.format = Format.CSV_HEADER;
            else if ("into".equals(key))
                options.target = value;
            else if ("threads".equals(key))
                options.nthreads = Integer.parseInt(value);
            else if ("commit".equals(key))
                options.commitFrequency = Long.parseLong(value);
            else if ("file".equals(key) ||
                     key.startsWith("file."))
                files.add(new File(dir, value));
            else if ("ddl".equals(key))
                ddlFile = new File(dir, value);
            else if ("count".equals(key))
                expectedCount = Long.parseLong(value);
            else if ("query".equals(key))
                query = value;
            else if ("expected".equals(key))
                expectedFile = new File(dir, value);
            else if ("retry".equals(key))
                options.maxRetries = Integer.parseInt(value);
            else if ("hosts".equals(key)) {
                options.hosts.clear();
                options.hosts.addAll(Arrays.asList(value.split(" ")));
            }
            else
                throw new Exception("Unknown property: " + key);
        }

        if (ddlFile != null)
            loadDDL(ddlFile);

        LoadClient client = new LoadClient(options);
        long count = 0;
        try {
            for (File file : files) {
                count += client.load(file);
            }
        }
        finally {
            client.clearConnections();
        }
        if (expectedCount >= 0)
            assertEquals("loaded count", expectedCount, count);

        if (query != null)
            checkQuery(query, expectedFile);
    }

    protected void loadDDL(File ddlFile) throws Exception {
        String ddl = fileContents(ddlFile);
        Connection conn = openConnection();
        Statement stmt = conn.createStatement();
        for (String sql : ddl.split("\\;\\s*")) {
            stmt.execute(sql);
        }
        stmt.close();
        conn.close();
    }

    protected void checkQuery(String query, File expectedFile) throws Exception {
        Connection conn = openConnection();
        Statement stmt = conn.createStatement();
        ResultSet rs = stmt.executeQuery(query);
        int ncols = rs.getMetaData().getColumnCount();
        StringBuilder str = new StringBuilder();
        while (rs.next()) {
            for (int i = 1; i <= ncols; i++) {
                if (i > 1) str.append('\t');
                str.append(rs.getString(i));
            }
            str.append('\n');
        }
        rs.close();
        stmt.close();
        conn.close();
        String actual = str.toString();
        String expected = fileContents(expectedFile);
        assertEquals(query, expected, actual);
    }

}
