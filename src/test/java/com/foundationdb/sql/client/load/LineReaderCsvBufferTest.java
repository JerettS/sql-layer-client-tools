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

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.List;

import org.junit.Test;

import static org.junit.Assert.*;

public class LineReaderCsvBufferTest {
    static final String encoding = "UTF-8";
    FileInputStream inputStream;

    @Test
    public void simpleRead () throws IOException {
        assertReadLines(list(list("field1", "field2")), "field1,field2");
    }

    @Test
    public void simpleReadTwoRows () throws IOException {
        assertReadLines(list(list("field1", "field2"),list("field3", "field4")), "field1,field2","field3,field4");
    }

    private static <T> List<T> list(T... values) {
        List<T> result = new ArrayList<>();
        for (T value : values) {
            result.add(value);
        }
        return result;
    }

    private static void assertReadLines(List<List<String>> expected, String... input) throws IOException {
        File file = tmpFileFrom(input);
        FileInputStream istr = null;
        try {
            istr = new FileInputStream(file);
            LineReader lines = new LineReader(istr.getChannel(), encoding, 1);
            CsvBuffer b = new CsvBuffer(encoding);
            List<List<String>> actual = new ArrayList<>();
            while (lines.readLine(b)) {
                actual.add(b.nextRow());
            }
            assertArrayEquals(expected.toArray(), actual.toArray());
        } finally {
            if (istr != null) {
                istr.close();
            }
        }
    }

    private static File tmpFileFrom(String... lines) throws IOException {
        File tmpFile = File.createTempFile(LineReaderQueryBufferTest.class.getSimpleName(), null);
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
}
