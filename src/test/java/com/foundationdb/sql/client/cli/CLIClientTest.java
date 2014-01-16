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

package com.foundationdb.sql.client.cli;

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
import java.util.*;

@RunWith(Parameterized.class)
public class CLIClientTest extends ClientTestBase
{
    public static final File RESOURCE_DIR = new File("src/test/resources/" + CLIClientTest.class.getPackage().getName().replace('.', '/'));
    public static final String SQL_PATTERN = ".*\\.sql";
    public static final String EXPECTED_EXTENSION = ".expected";

    public CLIClientOptions options = new CLIClientOptions();

    @Parameters(name="{0}")
    public static Collection<Object[]> sqlFiles() throws Exception {
        List<Object[]> result = new ArrayList<>();
        for (File sqlFile : listMatchingFiles(RESOURCE_DIR, SQL_PATTERN)) {
            String caseName = sqlFile.getName().replace(".sql", "");
            File expectedFile = new File(sqlFile.getParentFile(), caseName + EXPECTED_EXTENSION);
            result.add(new Object[] { caseName, sqlFile, expectedFile } );
        }
        return result;
    }

    protected final String caseName;
    protected final File sqlFile;
    protected final File expectedFile;

    public CLIClientTest(String caseName, File sqlFile, File expectedFile) {
        this.caseName = caseName;
        this.sqlFile = sqlFile;
        this.expectedFile = expectedFile;
    }

    @Before
    public void setSimpleJlineTerminal() {
        // Disable echo to avoid system-specific wrapping
        System.setProperty("jline.terminal", "none");
        fillBaseOptions(options);
        // To avoid a magic number of repetitions.
        options.schema = "test";
        options.urlOptions = "?prepareThreshold=1";
        options.includedParent = RESOURCE_DIR.getAbsolutePath();
    }

    @After
    public void cleanUp() throws Exception {
        dropSchema(options.schema);
    }

    @Test
    public void testTerminalSource() throws Exception {
        try(InputStream in = new BufferedInputStream(new FileInputStream(sqlFile));
            TerminalSource source = new TerminalSource("test", in, System.out)) {
            runAndCheck(source);
        }
    }

    @Test
    public void testFileSource() throws Exception {
        try(ReaderSource source = new ReaderSource(new FileReader(sqlFile.getAbsolutePath()))) {
            runAndCheck(source);
        }
    }

    private void runAndCheck(InputSource source) throws Exception {
        CharArrayWriter charWriter = new CharArrayWriter();
        WriterSink sink = new WriterSink(charWriter);
        try(CLIClient cli = new CLIClient(options)) {
            cli.openInternal(source, sink, false, false, false);
            cli.runLoop();
        }
        String expected = expectedFile.exists() ? fileContents(expectedFile).trim() : "";
        String actual = charWriter.toString().trim().replace("\r", "");
        assertEquals(caseName, expected, actual);
    }
}
