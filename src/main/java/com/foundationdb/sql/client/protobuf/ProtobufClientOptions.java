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

package com.foundationdb.sql.client.protobuf;

import com.beust.jcommander.IValueValidator;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParameterException;
import com.foundationdb.sql.client.ClientOptionsBase;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

public class ProtobufClientOptions extends ClientOptionsBase
{
    public static class DirectoryValidator implements IValueValidator<File>
    {
        @Override
        public void validate(String name, File value) throws ParameterException {
            if (!value.isDirectory()) {
                throw new ParameterException("Parameter " + name + " is not a directory");
            }
        }
    }


    @Parameter(names = { "-s", "--schema" }, description = "schema name")
    String schema = DEFAULT_SCHEMA;

    @Parameter(names = { "-o", "--output-file" }, description = "name of output file")
    File outputFile;

    @Parameter(names = { "-d", "--output-directory" }, description = "name of output directory", validateValueWith = DirectoryValidator.class)
    File outputDirectory;

    @Parameter(description = "group(s)", required = true)
    List<String> groups = new ArrayList<>();
}
