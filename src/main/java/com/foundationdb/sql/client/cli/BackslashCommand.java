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

public enum BackslashCommand
{
    CONNECT ("c", false, false, "[SCHEMA [USER [HOST [PORT]]]]", "change connection information"),
    CONNINFO("conninfo", false, false, "", "display connection information"),

    D_SEQUENCE  ("dq", true, true,  "[[SCHEMA] NAME]",          "Print description of sequences present"),
    D_TABLE     ("dt", true, true,  "[[SCHEMA] NAME]",          "Print description of tables present"),
    D_VIEW      ("dv", true, true,  "[[SCHEMA] NAME]",          "Print description of views present"),

    I_FILE      ("i", false, false, "FILE",                     "Execute commands from file"),
    O_FILE      ("o", false, false, "[FILE|-]",                 "Send all query results to file or standard out"),

    L_ALL       ("l",  true, false, "[[SCHEMA] NAME]",          "List tables, views and sequences"),
    L_INDEXES   ("li", true, true,  "[[[SCHEMA] TABLE] NAME]",  "List indexes"),
    L_SEQUENCES ("lq", true, true,  "[[SCHEMA] NAME]",          "List sequences"),
    L_SCHEMAS   ("ls", true, true,  "[NAME]",                   "List schemata"),
    L_TABLES    ("lt", true, true,  "[[SCHEMA] NAME]",          "List tables"),
    L_VIEWS     ("lv", true, true,  "[[SCHEMA] NAME]",          "List views"),

    TIMING  ("timing", false, false, "",                         "Toggles switch to display time per query in milliseconds"),
    HELP    ("?", false, false, "", "display this help"),
    QUIT    ("q", false, false, "", "quit"),
    ;

    public final String cmd;
    public final boolean hasSystem;
    public final boolean hasDetail;
    public final String helpCmd;
    public final String helpArgs;
    public final String helpDesc;
    public final boolean isDescribe;
    public final boolean isList;


    private BackslashCommand(String cmd, boolean hasSystem, boolean hasDetail, String helpArgs, String helpDesc) {
        this.cmd = cmd;
        this.helpCmd = "\\" + cmd + (hasSystem ? "[S]" : "") + (hasDetail ? "[+]" : "");
        this.hasSystem = hasSystem;
        this.hasDetail = hasDetail;
        this.helpArgs = helpArgs;
        this.helpDesc = helpDesc;
        this.isDescribe = cmd.charAt(0) == 'd';
        this.isList = cmd.charAt(0) == 'l';
    }
}
