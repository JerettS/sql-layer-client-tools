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

import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintStream;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

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

            "--skipRC", "-q", "-c", "select 5"
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

            "--skipRC", "-q", "-c","select 5;"
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

            "--skipRC", "-q", "-c", "select 5; select 6;"
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

            "--skipRC", "-q", "-f", tmpFile.getAbsolutePath()
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

            "--skipRC", "-q", "-f", tmpFile.getAbsolutePath()
        );
    }

    @Test
    public void backslashO() throws Exception {
        File outFile = tmpFileFrom();
        File inFile = tmpFileFrom(
            "SELECT 1;",
            "\\o " + outFile.getAbsolutePath(),
            "SELECT 2;",
            "SELECT 3;",
            "\\o",
            "SELECT 4;"
        );
        runAndCheck(
            null, // Only checking outFile contents

            "--skipRC", "-q", "-f", inFile.getAbsolutePath()
        );
        StringBuffer sb = new StringBuffer();
        try(BufferedReader reader = new BufferedReader(new FileReader(outFile))) {
            String l;
            while((l = reader.readLine()) != null) {
                sb.append(l);
                sb.append('\n');
            }
        }
        assertEquals(
            " _SQL_COL_1 \n" +
            "------------\n" +
            "          2 \n" +
            "(1 row)\n" +
            "\n" +
            " _SQL_COL_1 \n" +
            "------------\n" +
            "          3 \n" +
            "(1 row)\n" +
            "\n",
            sb.toString()
        );
    }

    @Test
    public void configurationFile() throws Exception {
        File tmpFile = tmpFileFrom(
                "select 1;"
        );
        File tmpFile2 = tmpFileFrom(
                "\\timing"
        );
        runAndVerify(
                "\\timing\n" +
                "select 1;\n" +
                        " _SQL_COL_1 \n" +
                        "------------\n" +
                        "          1 \n" +
                        "(1 row)\n" +
                        "\n"+
                        "Time to process query: ",

                "--rc", tmpFile2.getAbsolutePath(), "-q", "-f", tmpFile.getAbsolutePath()
        );
    }

    @Test
    public void switchTiming() throws Exception {
        File tmpFile = tmpFileFrom(
                "\\timing",
                "select 1;"
        );
        runAndVerify(
                "\\timing\n" +
                "select 1;\n" +
                        " _SQL_COL_1 \n" +
                        "------------\n" +
                        "          1 \n" +
                        "(1 row)\n" +
                        "\n" +
                        "Time to process query: ",

                "-r", "-q", "-f", tmpFile.getAbsolutePath()
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
            if(expected != null) {
                assertEquals(expected, testOut.toString());
            }
        } finally {
            System.setOut(origOut);
            System.setErr(origErr);
        }
    }
    private static void runAndVerify(String expected, String... args) throws Exception {
        PrintStream origOut = System.out;
        PrintStream origErr = System.err;
        try {
            ByteArrayOutputStream testOut = new ByteArrayOutputStream();
            System.setOut(new PrintStream(testOut));
            System.setErr(System.out);

            CLIClient.main(args);
            if(expected != null) {
                assertEquals(expected, testOut.toString().substring(0, expected.length()));
            }
        } finally {
            System.setOut(origOut);
            System.setErr(origErr);
        }
    }
}
