/**
 * Copyright (C) 2012-2014 FoundationDB, LLC
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

package com.foundationdb.sql.client.dump;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.converters.BaseConverter;
import com.foundationdb.sql.client.ClientOptionsBase;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

public class DumpClientOptions extends ClientOptionsBase
{
    public static final int DEFAULT_INSERT_MAX_ROW_COUNT = 100;
    public static final int COMMIT_AUTO = -1;

    public static class CommitConverter extends BaseConverter<Long>
    {
        public CommitConverter(String optionName) {
            super(optionName);
        }

        @Override
        public Long convert(String value) {
            return ("auto".equals(value) ? COMMIT_AUTO : Long.parseLong(value));
        }
    }


    @Parameter(names = { "-s", "--no-schemas" }, description = "omit DDL from output")
    public boolean noSchemas;

    @Parameter(names = { "-d", "--no-data" }, description = "omit data from output")
    public boolean noData;

    @Parameter(names = { "-o", "--output" }, description = "name of output file")
    public File outputFile;

    @Parameter(names = "--insert-max-rows", description = "number of rows per INSERT statement")
    public int insertMaxRowCount = DEFAULT_INSERT_MAX_ROW_COUNT;

    @Parameter(names = { "-c", "--commit" }, description = "commit every n rows", converter = CommitConverter.class)
    public Long commit;

    @Parameter(description = "schema(s)")
    public List<String> schemas = new ArrayList<>();
}
