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

import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintStream;

import static org.junit.Assert.assertEquals;

/** Test invoking main() directly. */
public class CLIClientMiscTest
{
    @Test
    public void dashCNoSemiColon() throws Exception {
        runAndCheck(
            " _SQL_COL_1 \n" +
            "------------\n" +
            "          5 \n" +
            "(1 row)\n"+
            "\n",

            "-q", "-c", "select 5"
        );
    }

    @Test
    public void dashCSingle() throws Exception {
        runAndCheck(
            " _SQL_COL_1 \n" +
            "------------\n" +
            "          5 \n" +
            "(1 row)\n"+
            "\n",

            "-q", "-c", "select 5;"
        );
    }

    @Test
    public void dashCMulti() throws Exception {
        runAndCheck(
            " _SQL_COL_1 \n" +
            "------------\n" +
            "          5 \n" +
            "(1 row)\n"+
            "\n"+
            " _SQL_COL_1 \n" +
            "------------\n" +
            "          6 \n" +
            "(1 row)\n"+
            "\n",

            "-q", "-c", "select 5; select 6;"
        );
    }

    @Test
    public void dashFMulti() throws Exception {
        File tmpFile = tmpFileFrom(
            "select 5;",
            "select 6;"
        );
        runAndCheck(
            "select 5;\n"+
            " _SQL_COL_1 \n" +
            "------------\n" +
            "          5 \n" +
            "(1 row)\n"+
            "\n"+
            "select 6;\n"+
            " _SQL_COL_1 \n" +
            "------------\n" +
            "          6 \n" +
            "(1 row)\n"+
            "\n",

            "-q", "-f", tmpFile.getAbsolutePath()
        );
    }

    @Test
    public void backslashIMissing() throws Exception {
        File tmpFile = tmpFileFrom(
            "select 1;",
            "\\i not_a_real_file",
            "select 2;"
        );
        runAndCheck(
            "select 1;\n" +
            " _SQL_COL_1 \n" +
            "------------\n" +
            "          1 \n" +
            "(1 row)\n" +
            "\n" +
            "\\i not_a_real_file\n" +
            "not_a_real_file (No such file or directory)\n" +
            "select 2;\n" +
            " _SQL_COL_1 \n" +
            "------------\n" +
            "          2 \n" +
            "(1 row)\n" +
            "\n",

            "-q", "-f", tmpFile.getAbsolutePath()
        );
    }


    private static File tmpFileFrom(String... lines) throws IOException {
        File tmpFile = File.createTempFile(CLIClientMiscTest.class.getSimpleName(), null);
        tmpFile.deleteOnExit();
        FileWriter writer = new FileWriter(tmpFile);
        for(String l : lines) {
            writer.write(l);
            writer.write('\n');
        }
        writer.flush();
        writer.close();
        return tmpFile;
    }

    private static void runAndCheck(String expected, String... args) throws Exception {
        PrintStream origOut = System.out;
        PrintStream origErr = System.err;
        try {
            ByteArrayOutputStream testOut = new ByteArrayOutputStream();
            System.setOut(new PrintStream(testOut));
            System.setErr(System.out);

            CLIClient.main(args);
            assertEquals(expected, testOut.toString());
        } finally {
            System.setOut(origOut);
            System.setErr(origErr);
        }
    }
}
