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

package com.akiban.client.dump;

/* (Not separate from server yet.)
import com.akiban.junit.NamedParameterizedRunner;
import com.akiban.junit.NamedParameterizedRunner.TestParameters;
import com.akiban.junit.Parameterization;
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
import java.util.regex.Pattern;

@RunWith(Parameterized.class)
public class DumpClientTest
{
    public static final File RESOURCE_DIR =
        new File("src/test/resources/"
                 + DumpClientTest.class.getPackage().getName().replace('.', '/'));
    public static final String SCHEMA_NAME = "dump_test";
    public static final String USER_NAME = "test";
    public static final String USER_PASSWORD = "test";

    @Parameters
    public static Collection<Object[]> dumps() throws Exception {
        List<Object[]> result = new ArrayList<Object[]>();
        for (File sqlFile : listSQLFiles(RESOURCE_DIR)) {
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
        openConnection().createStatement().execute("DROP SCHEMA IF EXISTS " + SCHEMA_NAME + " CASCADE");
    }

    @Test
    public void testLoadDump() throws Exception {
        // Take file from previous run, load it and dump again and
        // ensure it's the same.
        String loaded = fileContents(loadFile);

        Connection conn = openConnection();
        Statement stmt = conn.createStatement();
        for (String sql : loaded.split("\\;\\s*")) {
            try {
                stmt.execute(sql);
            }
            catch (SQLException ex) {
                if (sql.indexOf("DROP TABLE") < 0) // No IF EXISTS yet.
                    throw ex;
            }
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

    protected Connection openConnection() throws Exception {
        String url = String.format("jdbc:postgresql://%s:%d/%s",
                                   DumpClient.DEFAULT_HOST, DumpClient.DEFAULT_PORT,
                                   SCHEMA_NAME);
        Class.forName(DumpClient.DRIVER_NAME);
        return DriverManager.getConnection(url, USER_NAME, USER_PASSWORD);
    }

    public static File[] listSQLFiles(File dir) {
        File[] result = dir.listFiles(new RegexFilenameFilter(".*\\.sql"));
        Arrays.sort(result, new Comparator<File>() {
                        public int compare(File f1, File f2) {
                            return f1.getName().compareTo(f2.getName());
                        }
                    });
        return result;
    }

    public static String fileContents(File file) throws IOException {
        FileReader reader = null;
        try {
            reader = new FileReader(file);
            StringBuilder str = new StringBuilder();
            char[] buf = new char[128];
            while (true) {
                int nc = reader.read(buf);
                if (nc < 0) break;
                str.append(buf, 0, nc);
            }
            return str.toString();
        }
        finally {
            if (reader != null) {
                try {
                    reader.close();
                }
                catch (IOException ex) {
                }
            }
        }
    }

    public static class RegexFilenameFilter implements FilenameFilter
    {
        Pattern pattern;

        public RegexFilenameFilter(String regex) {
            this.pattern = Pattern.compile(regex);
        }

        @Override
        public boolean accept(File dir, String name) {
            return pattern.matcher(name).matches();
        }
    }

}
