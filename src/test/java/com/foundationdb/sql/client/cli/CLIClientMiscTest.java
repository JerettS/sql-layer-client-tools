/**
 * Copyright (C) 2009-2013 FoundationDB, LLC
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

package com.foundationdb.sql.client.cli;

import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileWriter;
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
        File tmpFile = File.createTempFile("clitest", "dashf");
        FileWriter writer = new FileWriter(tmpFile);
        writer.write("select 5;\n");
        writer.write("select 6;\n");
        writer.flush();
        writer.close();

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

    private void runAndCheck(String expected, String... args) throws Exception {
        PrintStream orig = System.out;
        try {
            ByteArrayOutputStream testOut = new ByteArrayOutputStream();
            System.setOut(new PrintStream(testOut));
            CLIClient.main(args);
            assertEquals(expected, testOut.toString());
        } finally {
            System.setOut(orig);
        }
    }
}
