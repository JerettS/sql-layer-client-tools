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

import org.junit.After;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.File;

import static org.junit.Assert.assertEquals;

public class CLIClientFileTest extends CLIClientTest
{
    public CLIClientFileTest(String caseName, File sqlFile, File expectedFile) {
        super(caseName, sqlFile, expectedFile);
    }

    @After
    public void clearFile() {
        OPTIONS.file = null;
    }

    @Override
    @Test
    public void test() throws Exception {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        CLIClient cli = new CLIClient(OPTIONS);
        cli.openInternal(null, out, sqlFile.getAbsolutePath(), false, false);
        try {
            cli.runLoop();
        } finally {
            cli.close();
        }
        String expected = expectedFile.exists() ? fileContents(expectedFile).trim() : "";
        String actual = out.toString("UTF-8").trim().replace("\r", "");
        assertEquals(caseName, expected, actual);
    }
}
