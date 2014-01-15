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

public abstract class ClientOptionsBase
{
    protected static String env(String env, String defValue) {
        String value = System.getenv(env);
        return (value != null) ? value : defValue;
    }

    public String getURL(String defaultSchema) {
        return String.format("jdbc:fdbsql://%s:%d/%s",
                             host,
                             port,
                             defaultSchema);
    }

    public void parseOrDie(String programName, String[] args) {
        try {
            JCommander jc = new JCommander(this, args);
            if(this.help) {
                jc.setProgramName(programName);
                jc.usage();
                System.out.println("If no schemas are given, all are dumped.");
                System.exit(0);
            }
        }
        catch(ParameterException ex) {
            System.out.println(ex.getMessage());
            System.exit(1);
        }
    }


    @Parameter(names = "--help", help = true)
    public boolean help;

    @Parameter(names = { "-h", "--host" }, description = "server host, name or IP")
    public String host = env("FDBSQL_HOST", "localhost");

    @Parameter(names = { "-p", "--port" }, description = "server port")
    public int port = Integer.parseInt(env("FDBSQL_PORT", "15432"));

    @Parameter(names = { "-u", "--user" }, description = "server user name")
    public String user = env("FDBSQL_USER", System.getProperty("user.name"));

    @Parameter(names = { "-w", "--password" }, description = "server user password")
    public String password = env("FDBSQL_PASSWORD", "");
}
