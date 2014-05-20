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
            "\\onerror CONTINUE success",
            "select from t;"
        );
        runAndCheck(false,
                0, 
            "\\onerror CONTINUE success\n" +
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
