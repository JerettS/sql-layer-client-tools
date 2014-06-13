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

import static org.junit.Assert.assertEquals;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintStream;

import org.junit.Test;

public class CLIClientExitTest {

    @Test
    public void exitSqlCode() throws Exception {
        File tmpFile = tmpFileFrom(
            "\\onerror exit sqlcode",
            "select from t;"
        );
        runAndCheck(false,
                146, 
            "\\onerror exit sqlcode\n" +
            "On-Error is EXIT SQLCODE\n" +
            "select from t;\n"+
            "ERROR: Encountered \" \"from\" \"from \"\" at line 1, column 8.\n"+
            "Was expecting one of:\n"+
            "    \"*\" ...\n" +
            "    \"**\" ...\n"+
            "    \n"
            + "\n",

            "--skip-rc", "-q", "-f", tmpFile.getAbsolutePath()
        );
    }
    
    @Test
    public void exitFailure() throws Exception {
        File tmpFile = tmpFileFrom(
            "\\onerror exit failure",
            "select from t;"
        );
        runAndCheck(false,
                1, 
            "\\onerror exit failure\n" +
            "On-Error is EXIT FAILURE\n" +
            "select from t;\n"+
            "ERROR: Encountered \" \"from\" \"from \"\" at line 1, column 8.\n"+
            "Was expecting one of:\n"+
            "    \"*\" ...\n" +
            "    \"**\" ...\n"+
            "    \n"
            + "\n",

            "--skip-rc", "-q", "-f", tmpFile.getAbsolutePath()
        );
    }

    
    @Test
    public void exitCode() throws Exception {
        File tmpFile = tmpFileFrom(
            "\\onerror exit 21",
            "select from t;"
        );
        runAndCheck(false,
                21, 
            "\\onerror exit 21\n" +
            "On-Error is EXIT CODE\n" +
            "select from t;\n"+
            "ERROR: Encountered \" \"from\" \"from \"\" at line 1, column 8.\n"+
            "Was expecting one of:\n"+
            "    \"*\" ...\n" +
            "    \"**\" ...\n"+
            "    \n"
            + "\n",

            "--skip-rc", "-q", "-f", tmpFile.getAbsolutePath()
        );
    }

    @Test
    public void exitSuccess() throws Exception {
        File tmpFile = tmpFileFrom(
            "\\onerror exit success",
            "select from t;"
        );
        runAndCheck(false,
                0, 
            "\\onerror exit success\n" +
            "On-Error is EXIT SUCCESS\n" +
            "select from t;\n"+
            "ERROR: Encountered \" \"from\" \"from \"\" at line 1, column 8.\n"+
            "Was expecting one of:\n"+
            "    \"*\" ...\n" +
            "    \"**\" ...\n"+
            "    \n"
            + "\n",

            "--skip-rc", "-q", "-f", tmpFile.getAbsolutePath()
        );
    }

    @Test
    public void continueSuccess() throws Exception {
        File tmpFile = tmpFileFrom(
            "\\onerror CONTINUE",
            "select from t;"
        );
        runAndCheck(false,
                0, 
            "\\onerror CONTINUE\n" +
            "On-Error is CONTINUE SUCCESS\n" +
            "select from t;\n"+
            "ERROR: Encountered \" \"from\" \"from \"\" at line 1, column 8.\n"+
            "Was expecting one of:\n"+
            "    \"*\" ...\n" +
            "    \"**\" ...\n"+
            "    \n"
            + "\n",

            "--skip-rc", "-q", "-f", tmpFile.getAbsolutePath()
        );
    }

    // NOTE: This test is correct. The \onerror CONTINUE 
    // uses only one parameter, but will accept two, 
    // the second being ignored. This test verifies this.
    @Test
    public void continueSqlcode() throws Exception {
        File tmpFile = tmpFileFrom(
            "\\onerror CONTINUE SQLCODE",
            "select from t;"
        );
        runAndCheck(false,
                0, 
            "\\onerror CONTINUE SQLCODE\n" +
            "On-Error is CONTINUE SUCCESS\n" +
            "select from t;\n"+
            "ERROR: Encountered \" \"from\" \"from \"\" at line 1, column 8.\n"+
            "Was expecting one of:\n"+
            "    \"*\" ...\n" +
            "    \"**\" ...\n"+
            "    \n"
            + "\n",

            "--skip-rc", "-q", "-f", tmpFile.getAbsolutePath()
        );
    }

    @Test
    public void exitDashSuccess() throws Exception {
        File tmpFile = tmpFileFrom(
            "select from t;"
        );
        runAndCheck(false,
                0, 
            "On-Error is CONTINUE SUCCESS\n" +
            "select from t;\n"+
            "ERROR: Encountered \" \"from\" \"from \"\" at line 1, column 8.\n"+
            "Was expecting one of:\n"+
            "    \"*\" ...\n" +
            "    \"**\" ...\n"+
            "    \n"
            + "\n",

            "--skip-rc", "-q", "--on-error", "CONTINUE", "SUCCESS", "-f", tmpFile.getAbsolutePath()
        );
    }
    
    @Test
    public void exitDashSQLCode() throws Exception {
        File tmpFile = tmpFileFrom(
            "select from t;"
        );
        runAndCheck(false,
                146, 
            "On-Error is EXIT SQLCODE\n" +
            "select from t;\n"+
            "ERROR: Encountered \" \"from\" \"from \"\" at line 1, column 8.\n"+
            "Was expecting one of:\n"+
            "    \"*\" ...\n" +
            "    \"**\" ...\n"+
            "    \n"
            + "\n",

            "--skip-rc", "-q", "--on-error", "EXIT", "SQLCODE", "-f", tmpFile.getAbsolutePath()
        );
    }
    
    @Test
    public void exitDashCode() throws Exception {
        File tmpFile = tmpFileFrom(
            "select from t;"
        );
        runAndCheck(false,
                21, 
            "On-Error is EXIT CODE\n" +
            "select from t;\n"+
            "ERROR: Encountered \" \"from\" \"from \"\" at line 1, column 8.\n"+
            "Was expecting one of:\n"+
            "    \"*\" ...\n" +
            "    \"**\" ...\n"+
            "    \n"
            + "\n",

            "--skip-rc", "-q", "--on-error", "EXIT", "21", "-f", tmpFile.getAbsolutePath()
        );
    }
    
    @Test
    public void exitDashCodeBad() throws Exception {
        File tmpFile = tmpFileFrom(
            "select from t;"
        );
        runAndCheck(false,
                0, 
            "Wrong onError status: xxdD, expected [SUCCESS|FAILURE|SQLCODE|<n>]\n" +
            "select from t;\n"+
            "ERROR: Encountered \" \"from\" \"from \"\" at line 1, column 8.\n"+
            "Was expecting one of:\n"+
            "    \"*\" ...\n" +
            "    \"**\" ...\n"+
            "    \n"
            + "\n",

            "--skip-rc", "-q", "--on-error", "EXIT", "xxdD", "-f", tmpFile.getAbsolutePath()
        );
    }

    @Test
    public void quitCode() throws Exception {
        File tmpFile = tmpFileFrom(
            "\\q 42"
        );
        runAndCheck(false,
                42, 
            "\\q 42" +    
            "\n",

            "--skip-rc", "-q", "-f", tmpFile.getAbsolutePath()
        );
    }
    
    @Test
    public void quitPlain() throws Exception {
        File tmpFile = tmpFileFrom(
            "\\q"
        );
        runAndCheck(false,
                0, 
            "\\q" +    
            "\n",

            "--skip-rc", "-q", "-f", tmpFile.getAbsolutePath()
        );
    }
    
    @Test
    public void exitInclude() throws Exception {
        File tmpFile1 = tmpFileFrom (
                "select from t;"
        );
        
        File tmpFile2 = tmpFileFrom (
                "\\onerror EXIT FAILURE",
                "\\i " + tmpFile1.getAbsolutePath(),
                "select * from t;"
        );
        
        runAndCheck(false,
            1,
            "\\onerror EXIT FAILURE\n" +
            "On-Error is EXIT FAILURE\n" +
            "\\i "+ tmpFile1.getAbsolutePath() + "\n" +                    
            "select from t;\n"+
            "ERROR: Encountered \" \"from\" \"from \"\" at line 1, column 8.\n"+
            "Was expecting one of:\n"+
            "    \"*\" ...\n" +
            "    \"**\" ...\n"+
            "    \n" +
            "\n",
            
            "--skip-rc", "-q", "-f", tmpFile2.getAbsolutePath()
        );
    }
    
    @Test
    public void testSetting() throws Exception {
        File tmpFile = tmpFileFrom(
                "\\onerror"
            );
            runAndCheck(false,
                    0, 
                "\\onerror\n" +
                "On-Error is CONTINUE SUCCESS\n",

                "--skip-rc", "-q", "-f", tmpFile.getAbsolutePath()
            );
    }
    
    @Test 
    public void testExitString() throws Exception {
        File tmpFile = tmpFileFrom("exit");
        runAndCheck(false,
                0,
                "\\q\n",
                "--skip-rc", "-q", "-f", tmpFile.getAbsolutePath()
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

    private static void runAndCheck(boolean startsWith, Integer exitCode, String expected,  String... args) throws Exception {
        PrintStream origOut = System.out;
        PrintStream origErr = System.err;
        try {
            ByteArrayOutputStream testOut = new ByteArrayOutputStream();
            System.setOut(new PrintStream(testOut));
            System.setErr(System.out);

            Integer result = CLIClient.test_main(args);
            
            assertEquals(exitCode, result);
            
            if(expected != null) {
                if (startsWith){
                    assertEquals(expected, testOut.toString().substring(0, expected.length()));                    
                } else {
                    assertEquals(expected, testOut.toString());
                }
            }
        } finally {
            System.setOut(origOut);
            System.setErr(origErr);
        }
    }
}
