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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.channels.FileChannel;

import org.junit.Test;

import com.foundationdb.sql.client.cli.QueryBuffer;

public class LineReaderQueryBufferTest {
    static final String encoding = "UTF-8";

    @Test
    public void simpleRead () throws IOException {
        File file = tmpFileFrom ("select * from t;");
        FileInputStream istr = new FileInputStream(file);
        LineReader lines = new LineReader(istr.getChannel(), encoding, 
                                          128, 1, 0, 1000);
        QueryBuffer b = new QueryBuffer();
        lines.readLine(b);
        
        assertTrue (b.hasQuery());
        
        istr.close();
    }
    
    @Test
    public void simpleBlockRead () throws IOException {
        File file = tmpFileFrom ("select * from t;");
        FileInputStream istr = new FileInputStream(file);
        LineReader lines = new LineReader(istr.getChannel(), encoding, 128); 
        QueryBuffer b = new QueryBuffer();
        
        while (lines.readLine(b)) {
            assertTrue (b.hasQuery());
        }
        
        istr.close();
    }
    
    @Test
    public void simpleMultiStatement () throws IOException {
        File file = tmpFileFrom ("create table `temp` (id int primary key); insert into `temp` values (1);");
        FileInputStream istr = new FileInputStream(file);
        LineReader lines = new LineReader(istr.getChannel(), encoding, 128); 
        QueryBuffer b = new QueryBuffer();
        
        while (lines.readLine(b)) {
            assertTrue (b.hasQuery());
            String create = b.nextQuery(); 
            assertEquals (create, "create table `temp` (id int primary key);");
            assertTrue (b.hasQuery());
            String insert = b.nextQuery();
            assertEquals (insert, " insert into `temp` values (1);");
            assertFalse(b.hasQuery());
        }
        
        istr.close();
    }

    @Test
    public void simpleMultiLine () throws IOException {
        File file = tmpFileFrom (
                "create table `temp` (id int primary key,",
                " name varchar(20) default 'no name',",
                " value varchar(21) not null);"
                );
        FileInputStream istr = new FileInputStream(file);
        LineReader lines = new LineReader(istr.getChannel(), encoding, 128); 
        QueryBuffer b = new QueryBuffer();
        
        while (lines.readLine(b)) {
            assertTrue (b.hasQuery());
            String create = b.nextQuery(); 
            assertEquals ("create table `temp` (id int primary key,  name varchar(20) default 'no name',  value varchar(21) not null);",
                    create);
        }
        istr.close();
    }

    @Test
    public void quotedSemicolon() throws IOException {
        File file = tmpFileFrom (
                "insert into `temp` values (",
                "'fred''s last stand;');"
                );
        FileInputStream istr = new FileInputStream(file);
        LineReader lines = new LineReader(istr.getChannel(), encoding, 128); 
        QueryBuffer b = new QueryBuffer();
        
        while (lines.readLine(b)) {
            assertTrue (b.hasQuery());
            String create = b.nextQuery(); 
            assertEquals ("insert into `temp` values ( 'fred''s last stand;');",
                    create);
        }
        istr.close();
    }
    
    @Test
    public void multipleStatements() throws IOException {
        File file = tmpFileFrom (
                "insert into `temp` values ('fred''s last stand;');",
                "insert into `temp` values ('thom''s over');"
                );
        FileInputStream istr = new FileInputStream(file);
        LineReader lines = new LineReader(istr.getChannel(), encoding, 128); 
        QueryBuffer b = new QueryBuffer();
        
        assertTrue (lines.readLine(b));
        assertTrue(b.hasQuery());
        String insert = b.nextQuery();
        assertEquals("insert into `temp` values ('fred''s last stand;');", insert);
        assertFalse(b.hasQuery());
        b.reset();
        assertTrue (lines.readLine(b));
        assertTrue (b.hasQuery());
        insert = b.nextQuery();
        assertEquals("insert into `temp` values ('thom''s over');", insert);
        assertFalse(b.hasQuery());
        assertFalse(lines.readLine(b));
        istr.close();
    }
    
    @Test
    public void comments1() throws IOException {
        File file = tmpFileFrom (
                "SELECT 4 -- zap",
                ";"
                );
        FileInputStream istr = new FileInputStream(file);
        LineReader lines = new LineReader(istr.getChannel(), encoding, 128); 
        QueryBuffer b = new QueryBuffer();
        assertTrue(lines.readLine(b));
        assertTrue(b.hasQuery());
        String query = b.nextQuery();
        assertEquals("SELECT 4  ;", query);
                
        istr.close();
    }

    @Test
    public void comments2() throws IOException {
        File file = tmpFileFrom (
                "SELECT 7 /* Awesome query */;"
                );
        FileInputStream istr = new FileInputStream(file);
        LineReader lines = new LineReader(istr.getChannel(), encoding, 128); 
        QueryBuffer b = new QueryBuffer();
        assertTrue(lines.readLine(b));
        assertTrue(b.hasQuery());
        String query = b.nextQuery();
        assertEquals("SELECT 7 /* Awesome query */;", query);
                
        istr.close();
    }
    
    
    @Test
    public void splitSimple() throws IOException {
        File file = tmpFileFrom (
                "insert into `temp` values ('fred''s last stand;');",
                "insert into `temp` values ('thom''s over');");
        FileInputStream istr = new FileInputStream(file);
        LineReader lines = new LineReader(istr.getChannel(), encoding, 1); 
        
        long mid = lines.splitParse(50L);
        assertEquals(mid, 51);
        
        lines = new LineReader (istr.getChannel(), encoding, FileLoader.SMALL_BUFFER_SIZE, 128, 0, 51);
        QueryBuffer b = new QueryBuffer();
        assertTrue (lines.readLine(b));
        assertTrue (b.hasQuery());
        String query = b.nextQuery();
        assertFalse(b.hasQuery());
        b.reset();
        assertFalse(lines.readLine(b));

        lines = new LineReader (istr.getChannel(), encoding, FileLoader.SMALL_BUFFER_SIZE, 128, 51, istr.getChannel().size());
        b = new QueryBuffer();
        assertTrue (lines.readLine(b));
        assertTrue (b.hasQuery());
        query = b.nextQuery();
        assertFalse(b.hasQuery());
        b.reset();
        assertFalse(lines.readLine(b));
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
