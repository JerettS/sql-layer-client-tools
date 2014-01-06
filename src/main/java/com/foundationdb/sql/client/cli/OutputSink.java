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

public interface OutputSink
{
    void print(char c) throws IOException;
    void print(String s) throws IOException;
    void println(String s) throws IOException;
    void println() throws IOException;

    void printError(char c) throws IOException;
    void printError(String s) throws IOException;
    void printlnError(String s) throws IOException;
    void printlnError() throws IOException;

    void flush() throws IOException;
}
