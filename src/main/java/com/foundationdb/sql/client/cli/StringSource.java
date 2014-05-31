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

import java.io.File;

public class StringSource implements InputSource
{
    private final String[] input;
    private int index;

    public StringSource(String... input) {
        if(input == null) {
            throw new NullPointerException("input");
        }
        this.input = input;
    }

    @Override
    public String getInfo() {
        return null;
    }

    @Override
    public void setPrompt(String prompt) {
        // Ignored
    }

    @Override
    public void openHistory(File file) {
        // Ignored
    }

    @Override
    public String addHistory(String input) {
        // Ignored
        return null;
    }

    @Override
    public String readSome() {
        return (index < input.length) ? input[index++] : null;
    }

    @Override
    public void close() {
        // None
    }
}
