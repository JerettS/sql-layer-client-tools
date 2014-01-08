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
import java.io.PrintStream;

public class PrintStreamSink implements OutputSink
{
    private final PrintStream out;
    private final PrintStream err;

    public PrintStreamSink(PrintStream out, PrintStream err) {
        this.out = out;
        this.err = err;
    }

    @Override
    public void close() throws IOException {
        out.close();
        err.close();
    }

    @Override
    public void print(char c) {
        out.print(c);
    }

    @Override
    public void print(String s) {
        out.print(s);
    }

    @Override
    public void println(String s) {
        out.println(s);
    }

    @Override
    public void println() {
        out.println();
    }

    @Override
    public void printError(char c) {
        err.print(c);
    }

    @Override
    public void printError(String s) {
        err.print(s);
    }

    @Override
    public void printlnError(String s) {
        err.println(s);
    }

    @Override
    public void printlnError() {
        err.println();
    }

    @Override
    public void flush() {
        out.flush();
        err.flush();
    }
}
