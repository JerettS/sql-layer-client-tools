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

package com.foundationdb.sql.client;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParameterException;

// Command line arguments override environment variables which override the literal defaults here
public abstract class ClientOptionsBase
{
    public static final String ENV_HOST_NAME = "FDBSQL_HOST";
    public static final String ENV_PORT_NAME = "FDBSQL_PORT";
    public static final String ENV_USER_NAME = "FDBSQL_USER";
    public static final String ENV_PASS_NAME = "FDBSQL_PASSWORD";
    public static final String ENV_SCHEMA_NAME = "FDBSQL_SCHEMA";

    public static final String DEFAULT_HOST = env(ENV_HOST_NAME, "localhost");
    public static final int DEFAULT_PORT = Integer.parseInt(env(ENV_PORT_NAME, "15432"));
    public static final String DEFAULT_USER = env(ENV_USER_NAME, System.getProperty("user.name"));
    public static final String DEFAULT_PASS = env(ENV_PASS_NAME, "");
    public static final String DEFAULT_SCHEMA = env(ENV_SCHEMA_NAME, DEFAULT_USER);


    protected static String env(String env, String defValue) {
        String value = System.getenv(env);
        return (value != null) ? value : defValue;
    }

    protected void printExtraHelp() {
        // None
    }

    public String getURL(String schema) {
        return formatURL(getHost(), port, schema);
    }

    public static String formatURL(String host, int port, String schema) {
        return String.format("jdbc:fdbsql://%s:%d/%s", host, port, schema);
    }

    public void parseOrDie(String programName, String[] args) {
        try {
            JCommander jc = new JCommander(this, args);
            if(this.help == null) {
                this.help = false;
            }
            if(this.help) {
                jc.setProgramName(programName);
                jc.usage();
                printExtraHelp();
                System.exit(0);
            }
            if(this.version) {
                System.out.printf("%s %s\n", programName, Version.VERSION_SHORT);
                System.exit(0);
            }
        }
        catch(ParameterException ex) {
            System.out.println(ex.getMessage());
            System.exit(1);
        }
    }

    public abstract String getHost();
    public abstract void setHost(String host);


    @Parameter(names = "--help", description = "show this help", help = true)
    public Boolean help; // Object so there is no default in the help output

    @Parameter(names = { "-p", "--port" }, description = "server port")
    public int port = DEFAULT_PORT;

    @Parameter(names = { "-u", "--user" }, description = "server user name")
    public String user = DEFAULT_USER;

    @Parameter(names = { "--version" }, description = "output version information and exit", help = true)
    public boolean version;

    @Parameter(names = { "-w", "--password" }, description = "server user password")
    public String password = DEFAULT_PASS;
}
