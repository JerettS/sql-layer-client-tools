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

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.Reader;

public class ReaderSource implements InputSource
{
    private final BufferedReader input;

    public ReaderSource(Reader reader) {
        this.input = new BufferedReader(reader);
    }

    @Override
    public String getInfo() {
        return null;
    }

    @Override
    public void setPrompt(String prompt) {
        // Ignore
    }

    @Override
    public void openHistory(File file) {
        // Ignore
    }

    @Override
    public void addHistory(String line) {
        // Ignore
    }

    @Override
    public String readLine() throws IOException {
        return input.readLine();
    }

    @Override
    public void close() {
        try {
            input.close();
        } catch(IOException e) {
            System.err.println(e.getMessage());
        }
    }
}
