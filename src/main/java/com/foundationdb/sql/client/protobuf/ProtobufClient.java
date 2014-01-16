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

package com.foundationdb.sql.client.protobuf;

import org.postgresql.copy.CopyManager;

import java.io.*;
import java.sql.*;

public class ProtobufClient
{
    private static final String PROGRAM_NAME = "fdbsqlprotod";

    private ProtobufClientOptions options;
    private Writer output;
    private Connection connection;
    private CopyManager copyManager;


    public static void main(String[] args) throws Exception {
        ProtobufClientOptions options = new ProtobufClientOptions();
        options.parseOrDie(PROGRAM_NAME, args);
        if ((options.outputFile != null) && (options.outputDirectory != null)) {
            System.err.println("Cannot specify both output file and output directory");
            System.exit(1);
        }
        if ((options.outputFile != null) && (options.groups.size() > 1)) {
            System.err.println("Cannot specify output file for more than one group");
            System.exit(1);
        }
        ProtobufClient protoClient = new ProtobufClient(options);
        protoClient.writeProtobuf();
    }


    public ProtobufClient(ProtobufClientOptions options) {
        this.options = options;
    }

    public void close() {
        closeConnection();
        closeOutput();
    }

    public void writeProtobuf() throws Exception {
        try {
            openConnection();
            for (String group : options.groups) {
                String schemaName, groupName;
                int idx = group.indexOf('.');
                if (idx < 0) {
                    schemaName = null;
                    groupName = group;
                }
                else {
                    schemaName = group.substring(0, idx);
                    groupName = group.substring(idx+1);
                }
                openOutput(groupName);
                writeGroup(schemaName, groupName);
                closeOutput();
                if (options.outputFile != null)
                    System.out.println(String.format("Wrote %s to %s.", 
                                                     group,
                                                     options.outputFile));
            }
        }
        finally {
            closeConnection();
        }
    }

    protected void writeGroup(String schemaName, String groupName) 
            throws SQLException, IOException {
        StringBuilder sql = new StringBuilder("CALL sys.group_protobuf(");
        if (schemaName == null)
            sql.append("null");
        else
            sql.append("'").append(schemaName.replace("'", "''")).append("'");
        sql.append(",'");
        sql.append(groupName.replace("'", "''"));
        sql.append("')");
        copyManager.copyOut(sql.toString(), output);
    }

    protected void openOutput(String name) throws Exception {
        if (options.outputDirectory != null)
            options.outputFile = new File(options.outputDirectory, name + ".proto");
        if (options.outputFile != null)
            output = new OutputStreamWriter(new FileOutputStream(options.outputFile), "UTF-8");
        else
            output = new OutputStreamWriter(System.out);
    }

    protected void closeOutput() {
        if (output != null) {
            try {
                output.close();
            }
            catch (IOException ex) {
            }
            output = null;
        }
    }

    protected void openConnection() throws Exception {
        String url = options.getURL(options.schema);
        connection = DriverManager.getConnection(url, options.user, options.password);
        copyManager = new CopyManager((org.postgresql.core.BaseConnection)connection);
    }

    protected void closeConnection() {
        if (connection != null) {
            try {
                connection.close();
            }
            catch (SQLException ex) {
            }
            connection = null;
        }
    }

}
