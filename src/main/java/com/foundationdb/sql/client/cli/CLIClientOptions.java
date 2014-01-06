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

import java.io.File;
import java.util.ArrayList;
import java.util.List;

// Command line arguments override environment variables which override the literal defaults here
public class CLIClientOptions
{
    @Parameter(names = "--help", help = true)
    boolean help;

    @Parameter(names = { "-h", "--host" }, description = "server host, name or IP")
    String host = env("FDBSQL_HOST", "localhost");

    @Parameter(names = { "-p", "--port" }, description = "server port")
    int port = Integer.parseInt(env("FDBSQL_PORT", "15432"));

    @Parameter(names = { "-u", "--user" }, description = "server user name")
    String user = env("FDBSQL_USER", System.getProperty("user.name"));

    @Parameter(names = { "-w", "--password" }, description = "server user password")
    String password = env("FDBSQL_PASSWORD", "");

    @Parameter(names = { "-s", "--schema" }, description = "server user name")
    String schema = env("FDBSQL_SCHEMA", user);

    @Parameter(names = { "-c", "--command" }, description = "execute command(s) and then exit")
    String command = null;

    @Parameter(names = { "-f", "--file" }, description = "execute commands from file and then exit")
    String file = null;

    @Parameter(names = { "-q", "--quiet" }, description = "output only query results")
    boolean quiet = false;

    @Parameter(description="[schema]")
    public List<String> positional = new ArrayList<>();


    private static String env(String env, String defValue) {
        String value = System.getenv(env);
        return (value != null) ? value : defValue;
    }
}