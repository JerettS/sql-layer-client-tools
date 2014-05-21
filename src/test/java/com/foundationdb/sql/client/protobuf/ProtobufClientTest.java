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

package com.foundationdb.sql.client.protobuf;

import com.foundationdb.sql.client.ClientTestBase;

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
public class ProtobufClientTest extends ClientTestBase
{
    public static final File RESOURCE_DIR =
        new File("src/test/resources/"
                 + ProtobufClientTest.class.getPackage().getName().replace('.', '/'));
    public static final String SQL_PATTERN = ".*\\.sql";
    private static final String UUID_REGEX =
        "[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}";

    @Parameters
    public static Collection<Object[]> protobufs() throws Exception {
        List<Object[]> result = new ArrayList<Object[]>();
        for (File sqlFile : listMatchingFiles(RESOURCE_DIR, SQL_PATTERN)) {
            String caseName = sqlFile.getName().replace(".sql", "");
            result.add(new Object[] { caseName, sqlFile });
        }
        return result;
    }

    private String caseName;
    private File loadFile;

    public ProtobufClientTest(String caseName, File loadFile) {
        this.caseName = caseName;
        this.loadFile = loadFile;
    }

    @Before
    @After
    public void cleanUp() throws Exception {
        dropSchema();
    }

    @Test
    public void testLoadProtobuf() throws Exception {
        String ddl = fileContents(loadFile);

        Connection conn = openConnection();
        StatementHelper helper = new StatementHelper(conn);
        for (String sql : ddl.split("\\;\\s*")) {
            helper.execute(sql, true);
        }
        helper.close();
        conn.close();

        File protoFile = File.createTempFile("test-", ".proto");
        protoFile.deleteOnExit();

        ProtobufClientOptions options = new ProtobufClientOptions();
        fillBaseOptions(options);
        options.schema = SCHEMA_NAME;
        options.outputFile = protoFile;
        options.groups.add(loadFile.getName().replace(".sql", ""));

        ProtobufClient client = new ProtobufClient(options);
        try {
            client.writeProtobuf();
        } finally {
            client.close();
        }
        
        String proto = fileContents(protoFile);
        String expected = fileContents(new File(loadFile.getParentFile(),
                                                loadFile.getName().replace(".sql", 
                                                                           ".proto")));

        proto = proto.replaceAll(UUID_REGEX, "*uuid*");
        expected = expected.replaceAll(UUID_REGEX, "*uuid*");

        assertEquals(caseName, expected, proto);
    }

}
