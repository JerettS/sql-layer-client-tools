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

import org.junit.Test;
import static org.junit.Assert.*;

import java.io.*;

public class LineReaderTest
{
    static final String encoding = "UTF-8";

    @Test
    public void readWhole() throws Exception {
        File file = writeLines(3, 2, "\n");
        FileInputStream istr = new FileInputStream(file);
        LineReader lines = new LineReader(istr.getChannel(), encoding);
        StringBuilder str = new StringBuilder();
        while (true) {
            if (!lines.readLine(str)) break;
            str.append("\n");
        }
        assertEquals(" 1\n 2\n 3\n", str.toString());
        istr.close();
    }

    @Test
    public void accuratePosition() throws Exception {
        File file = writeLines(3, 2, "\n");
        FileInputStream istr = new FileInputStream(file);
        LineReader lines = new LineReader(istr.getChannel(), encoding, 1);
        assertEquals(" 1", lines.readLine());
        assertEquals(3, lines.position());
        istr.close();
    }

    @Test
    public void readBounded() throws Exception {
        File file = writeLines(10, 4, "\n");
        FileInputStream istr = new FileInputStream(file);
        LineReader lines = new LineReader(istr.getChannel(), encoding,
                                          128, 128,
                                          10, 20);
        assertEquals("   3", lines.readLine());
        assertEquals("   4", lines.readLine());
        assertEquals(null, lines.readLine());
        istr.close();
    }

    @Test
    public void crlf() throws Exception {
        File file = writeLines(2, 2, "\r\n");
        FileInputStream istr = new FileInputStream(file);
        LineReader lines = new LineReader(istr.getChannel(), encoding);
        assertEquals(" 1", lines.readLine());
        assertEquals(" 2", lines.readLine());
        assertEquals(null, lines.readLine());
        istr.close();
    }

    @Test
    public void findSimple() throws Exception {
        File file = writeLines(100, 4, "\n");
        FileInputStream istr = new FileInputStream(file);
        LineReader lines = new LineReader(istr.getChannel(), encoding, 
                                          128, 1, 100, 300);
        assertEquals(230, lines.newLineNear(232));
        assertEquals(230, lines.newLineNear(228));
        istr.close();
    }

    @Test
    public void findLongLines() throws Exception {
        File file = writeLines(10, 999, "\n");
        FileInputStream istr = new FileInputStream(file);
        LineReader lines = new LineReader(istr.getChannel(), encoding, 
                                          128, 1, 1000, 8000);
        assertEquals(2000, lines.newLineNear(1999));
        assertEquals(2000, lines.newLineNear(2001));
        assertEquals(2000, lines.newLineNear(1501));
        assertEquals(2000, lines.newLineNear(2499));
        istr.close();
    }

    
    protected File writeLines(int nlines, int lineWidth, String nl) throws IOException {
        File file = File.createTempFile("lines", ".txt");
        file.deleteOnExit();
        FileOutputStream ostr = new FileOutputStream(file);
        for (int i = 0; i < nlines; i++) {
            byte[] b = String.format("%d", i+1).getBytes(encoding);
            for (int j = b.length; j < lineWidth; j++) {
                ostr.write((byte)' ');
            }
            ostr.write(b);
            ostr.write(nl.getBytes(encoding));
        }
        return file;
    }
}
