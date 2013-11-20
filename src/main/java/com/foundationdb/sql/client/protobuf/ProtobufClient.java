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

import com.beust.jcommander.converters.BaseConverter;
import com.beust.jcommander.IValueValidator;
import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParameterException;

import org.postgresql.copy.CopyManager;

import java.io.*;
import java.sql.*;
import java.util.*;

public class ProtobufClient
{
    protected static final String DRIVER_NAME = "org.postgresql.Driver";
    protected static final String DEFAULT_HOST = "localhost";
    protected static final int DEFAULT_PORT = 15432;
    protected static final String DEFAULT_USER = "system";
    protected static final String DEFAULT_PASSWORD = "system";
    protected static final String DEFAULT_SCHEMA = System.getProperty("user.name");
    private File outputFile = null;
    private File outputDirectory = null;
    private String host = DEFAULT_HOST;
    private int port = DEFAULT_PORT;
    private String user = DEFAULT_USER;
    private String password = DEFAULT_PASSWORD;
    private String defaultSchema = DEFAULT_SCHEMA;
    private List<String> groups = new ArrayList<>();
    private Writer output;
    private Connection connection;
    private CopyManager copyManager;

    public static class DirectoryValidator implements IValueValidator<File> {
        @Override
        public void validate(String name, File value) throws ParameterException {
            if (!value.isDirectory()) {
                throw new ParameterException("Parameter " + name + " is not a directory");
            }
        }
    }

    static class CommandOptions {
        @Parameter(names = "--help", help = true)
        boolean help;

        @Parameter(description = "group(s)")
        List<String> groups = new ArrayList<>();

        @Parameter(names = { "-o", "--output-file" }, description = "name of output file")
        File outputFile;

        @Parameter(names = { "-d", "--output-directory" }, description = "name of output directory", validateValueWith = DirectoryValidator.class)
        File outputDirectory;

        @Parameter(names = { "-h", "--host" }, description = "name of server host")
        String host = DEFAULT_HOST;

        @Parameter(names = { "-p", "--port" }, description = "Postgres server port")
        int port = DEFAULT_PORT;
        
        @Parameter(names = { "-u", "--user" }, description = "server user name")
        String user = DEFAULT_USER;

        @Parameter(names = { "-w", "--password" }, description = "server user password")
        String password = DEFAULT_PASSWORD;

        @Parameter(names = { "-s", "--schema" }, description = "schema name")
        String schema = DEFAULT_SCHEMA;
    }

    public static void main(String[] args) throws Exception {
        CommandOptions options = new CommandOptions();
        JCommander jc;
        try {
            jc = new JCommander(options, args);
        }
        catch (ParameterException ex) {
            System.out.println(ex.getMessage());
            return;
        }
        if (options.help) {
            jc.setProgramName("fdbsqlprotod");
            jc.usage();
            return;
        }
        if (options.groups.isEmpty()) {
            System.out.println("No groups were specified");
            return;
        }
        if ((options.outputFile != null) && (options.outputDirectory != null)) {
            System.out.println("Cannot specify both output file and output directory");
        }
        if ((options.outputFile != null) && (options.groups.size() > 1)) {
            System.out.println("Cannot specify output file for more than one group");
        }
        ProtobufClient protoClient = new ProtobufClient(options);
        protoClient.writeGroups();
    }

    public ProtobufClient() {
    }

    public ProtobufClient(CommandOptions options) {
        if (options.outputFile != null)
            setOutputFile(options.outputFile);
        if (options.outputDirectory != null)
            setOutputDirectory(options.outputDirectory);
        setHost(options.host);
        setPort(options.port);
        setUser(options.user);
        setPassword(options.password);
        setDefaultSchema(options.schema);
        for (String group : options.groups) {
            addGroup(group);
        }
    }

    public File getOutputFile() {
        return outputFile;
    }
    public void setOutputFile(File outputFile) {
        this.outputFile = outputFile;
    }
    public File getOutputDirectory() {
        return outputDirectory;
    }
    public void setOutputDirectory(File outputDirectory) {
        this.outputDirectory = outputDirectory;
    }
    public String getHost() {
        return host;
    }
    public void setHost(String host) {
        this.host = host;
    }
    public int getPort() {
        return port;
    }
    public void setPort(int port) {
        this.port = port;
    }
    public String getUser() {
        return user;
    }
    public void setUser(String user) {
        this.user = user;
    }
    public String getPassword() {
        return password;
    }
    public void setPassword(String password) {
        this.password = password;
    }
    public String getDefaultSchema() {
        return defaultSchema;
    }
    public void setDefaultSchema(String defaultSchema) {
        this.defaultSchema = defaultSchema;
    }

    public Collection<String> getGroups() {
        return groups;
    }
    public void addGroup(String group) {
        groups.add(group);
    }

    public void writeGroups() throws Exception {
        try {
            openConnection();
            for (String group : groups) {
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
                if (outputFile != null)
                    System.out.println(String.format("Wrote %s to %s.", 
                                                     group, outputFile));
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
        if (outputDirectory != null)
            outputFile = new File(outputDirectory, name + ".proto");
        if (outputFile != null)
            output = new OutputStreamWriter(new FileOutputStream(outputFile), "UTF-8");
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
        Class.forName(DRIVER_NAME);
        String url = String.format("jdbc:postgresql://%s:%d/%s", 
                                   host, port, defaultSchema);
        connection = DriverManager.getConnection(url, user, password);
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
