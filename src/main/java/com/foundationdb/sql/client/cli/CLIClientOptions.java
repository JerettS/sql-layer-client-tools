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

import com.beust.jcommander.Parameter;
import com.foundationdb.sql.client.ClientOptionsBase;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

public class CLIClientOptions extends ClientOptionsBase
{
    @Parameter(names = { "-s", "--schema" }, description = "schema name")
    public String schema = DEFAULT_SCHEMA;

    @Parameter(names = { "-c", "--command" }, description = "execute command(s) and then exit")
    public String command = null;

    @Parameter(names = { "-f", "--file" }, description = "execute commands from file and then exit")
    public String file = null;

    @Parameter(names = { "-q", "--quiet" }, description = "output only query results")
    public boolean quiet = false;

    @Parameter(description="[schema]")
    public List<String> positional = new ArrayList<>();

    //
    // Used by tests but not otherwise exposed.
    //

    // Appended to JDBC URL.
    String urlOptions = "";
    // Used as the parent directory for any \i file
    String includedParent = null;
}