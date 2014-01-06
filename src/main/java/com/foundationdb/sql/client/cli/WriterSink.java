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

import java.io.IOException;
import java.io.Writer;

public class WriterSink implements OutputSink
{
    private static final char[] NL = System.getProperty("line.separator").toCharArray();
    private final Writer out;
    private final Writer err;

    public WriterSink(Writer out) {
        this(out, out);
    }

    public WriterSink(Writer out, Writer err) {
        this.out = out;
        this.err = err;
    }

    @Override
    public void print(char c) throws IOException {
        out.write(c);
    }

    @Override
    public void print(String s) throws IOException {
        out.write(s);
    }

    @Override
    public void println(String s) throws IOException {
        print(s);
        println();
    }

    @Override
    public void println() throws IOException {
        out.write(NL);
    }

    @Override
    public void printError(char c) throws IOException {
        err.write(c);
    }

    @Override
    public void printError(String s) throws IOException {
        err.write(s);
    }

    @Override
    public void printlnError(String s) throws IOException {
        err.write(s);
        printlnError();
    }

    @Override
    public void printlnError() throws IOException {
        err.write(NL);
    }

    @Override
    public void flush() throws IOException {
        out.flush();
        err.flush();
    }
}
