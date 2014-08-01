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
import com.beust.jcommander.Parameters;
import com.foundationdb.sql.client.ClientOptionsBase;

import java.util.ArrayList;
import java.util.List;

@Parameters (separators = "=")
public class CLIClientOptions extends ClientOptionsBase
{
    @Override
    public String getHost() {
        return host;
    }

    @Override
    public void setHost(String host) {
        this.host = host;
    }

    @Parameter(names = { "-h", "--host" }, description = "server host, name or IP")
    public String host = DEFAULT_HOST;

    @Parameter(names = { "-s", "--schema" }, description = "schema name")
    public String schema = DEFAULT_SCHEMA;

    @Parameter(names = { "-c", "--command" }, description = "execute command(s) and then exit")
    public String command = null;

    @Parameter(names = { "-f", "--file" }, description = "execute commands from file and then exit")
    public String file = null;
    
    @Parameter(names = {"-o", "--output"}, description = "output results to file" )
    public String output = null;

    @Parameter(names = { "-q", "--quiet" }, description = "output only query results")
    public boolean quiet = false;

    @Parameter(names = {"--skip-rc"}, description = "skips run of configuration file at start")
    public Boolean skipRC = false;
    
    @Parameter(names = {"--rc"}, description = "set configuration file to new path")
    public String configFileName = System.getProperty("user.home") + "/.fdbsqlclirc";

    @Parameter(names = "--on-error", arity = 2, description = "on error handling")
    public List<String> onError = new ArrayList<>();
    
    @Parameter(description="[schema]")
    public List<String> positional = new ArrayList<>();

    //
    // Used by tests but not otherwise exposed.
    //

    // Appended to JDBC URL.
    String urlOptions = "";
    // Used as the parent directory for any \i file
    String includedParent = null;
    OnErrorType onErrorType = OnErrorType.CONTINUE;
    OnErrorStatus onErrorStatus = OnErrorStatus.SUCCESS;
    Integer statusCode = 0;

    protected enum OnErrorType {
        CONTINUE, 
        EXIT;

        public static OnErrorType fromString(String text) {
            if (text != null) {
                for (OnErrorType t : OnErrorType.values()) {
                    if (text.equalsIgnoreCase(t.name())) {
                        return t;
                    }
                }
            }
            return null;
        }       
    }
    
    protected enum OnErrorStatus {
        SUCCESS (0),
        FAILURE (1),
        SQLCODE (-1),
        CODE (-2),;
        
        public final Integer statusValue;
        
        private OnErrorStatus (int status) {
            statusValue = status;
        }
        public static OnErrorStatus fromString(String text) {
            if (text != null) {
                for (OnErrorStatus s : OnErrorStatus.values()) {
                    if (text.equalsIgnoreCase(s.name())) {
                        return s;
                    }
                }
                try  {
                    Integer value = Integer.parseInt(text);
                    if (value >= 0 && value <= 255) {
                        return OnErrorStatus.CODE;
                    }
                } catch (NumberFormatException e) {
                    // Explicitly do nothing. 
                }
            }
            return null;
        }       
    }
}