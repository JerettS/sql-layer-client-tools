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

package com.foundationdb.sql.client.load;

import com.beust.jcommander.converters.BaseConverter;
import com.beust.jcommander.IParameterValidator;
import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParameterException;

import org.postgresql.copy.CopyManager;

import java.io.*;
import java.nio.channels.FileChannel;
import java.sql.*;
import java.util.*;
import java.util.concurrent.ConcurrentLinkedDeque;

public class LoadClient
{
    public static enum Format {
        AUTO("auto"), CSV("CSV"), CSV_HEADER("CSV with header"), MYSQL_DUMP("MySQL"), FDB_SQL("SQL");

        final String name;
        Format(String name) {
            this.name = name;
        }

        @Override
        public String toString() {
            return name;
        }

        public static Format fromName(String name) {
            for (Format fmt : values()) {
                if (name.equalsIgnoreCase(fmt.name)) {
                    return fmt;
                }
            }
            throw new IllegalArgumentException("Unknown format: " + name);
        }
    }

    protected static final String DRIVER_NAME = "org.postgresql.Driver";
    protected static final String DEFAULT_HOST = "localhost";
    protected static final int DEFAULT_PORT = 15432;
    protected static final String DEFAULT_USER = "system";
    protected static final String DEFAULT_PASSWORD = "system";
    protected static final String DEFAULT_SCHEMA = System.getProperty("user.name");
    protected static final int COMMIT_AUTO = -1;
    private String host = DEFAULT_HOST;
    private int port = DEFAULT_PORT;
    private String user = DEFAULT_USER;
    private String password = DEFAULT_PASSWORD;
    private String schema = DEFAULT_SCHEMA;
    private String target = null;
    private Format format = Format.AUTO;
    private String encoding = "UTF-8";
    private int nthreads = 1;
    private long commitFrequency = 0;
    private int maxRetries = 1;
    private boolean quiet = false;
    private String constraintCheckTime = "DEFERRED_WITH_RANGE_CACHE";
    private Deque<Connection> connections = new ConcurrentLinkedDeque<>();

    public static class FormatConverter extends BaseConverter<Format> {
        public FormatConverter(String optionName) {
            super(optionName);
        }

        @Override
        public Format convert(String value) {
            return Format.fromName(value);
        }
    }

    public static class CommitConverter extends BaseConverter<Long> {
        public CommitConverter(String optionName) {
            super(optionName);
        }

        @Override
        public Long convert(String value) {
            return ("auto".equals(value) ? COMMIT_AUTO : Long.parseLong(value));
        }
    }

    public static class ConstraintCheckTimeValidator implements IParameterValidator {
        @Override
        public void validate(String name, String value) throws ParameterException {
            if (!value.matches("\\w+")) {
                throw new ParameterException("Parameter " + name + " is not a keyword");
            }
        }
    }

    static class CommandOptions {
        @Parameter(names = "--help", help = true)
        boolean help;

        @Parameter(description = "file(s)", required = true)
        List<File> files = new ArrayList<>();

        @Parameter(names = { "-h", "--host" }, description = "name of server host")
        String host = DEFAULT_HOST;

        @Parameter(names = { "-p", "--port" }, description = "Postgres server port")
        int port = DEFAULT_PORT;
        
        @Parameter(names = { "-u", "--user" }, description = "server user name")
        String user = DEFAULT_USER;

        @Parameter(names = { "-w", "--password" }, description = "server user password")
        String password = DEFAULT_PASSWORD;

        @Parameter(names = { "-s", "--schema" }, description = "destination schema")
        String schema = DEFAULT_SCHEMA;

        @Parameter(names = { "-f", "--format" }, description = "file format", converter = FormatConverter.class)
        Format format = Format.AUTO;

        @Parameter(names = "--header", description = "CSV file has header")
        boolean header;
        
        @Parameter(names = { "-t", "--into" }, description = "target table name")
        String target;

        @Parameter(names = { "-n", "--threads" }, description = "number of threads")
        int threads = 1;

        @Parameter(names = { "-c", "--commit" }, description = "commit every n rows", converter = CommitConverter.class)
        Long commit;

        @Parameter(names = { "-r", "--retry" }, description = "number of times to try on transaction error")
        int retry = 1;

        @Parameter(names = { "-q", "--quiet" }, description = "no progress output")
        boolean quiet;

        @Parameter(names = { "--constraint-check-time" }, description = "when to check uniqueness constraints", validateWith = ConstraintCheckTimeValidator.class)
        String constraintCheckTime = "DEFERRED_WITH_RANGE_CACHE";
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
            jc.setProgramName("fdbsqlload");
            jc.usage();
            return;
        }
        Class.forName(DRIVER_NAME);
        LoadClient loadClient = new LoadClient(options);
        try {
            for (File file : options.files) {
                loadClient.load(file);
            }
        }
        finally {
            loadClient.clearConnections();
        }
    }

    public LoadClient() {
    }

    public LoadClient(CommandOptions options) {
        setHost(options.host);
        setPort(options.port);
        setUser(options.user);
        setPassword(options.password);
        setSchema(options.schema);
        setFormat(options.format);
        setTarget(options.target);
        setThreads(options.threads);
        if (options.commit != null)
            setCommitFrequency(options.commit);
        setMaxRetries(options.retry);
        setQuiet(options.quiet);
        setConstraintCheckTime(options.constraintCheckTime);
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
    public String getSchema() {
        return schema;
    }
    public void setSchema(String schema) {
        this.schema = schema;
    }
    public String getTarget() {
        return target;
    }
    public void setTarget(String target) {
        this.target = target;
    }
    public Format getFormat() {
        return format;
    }
    public void setFormat(Format format) {
        this.format = format;
    }
    public String getEncoding() {
        return encoding;
    }
    public void setEncoding(String encoding) {
        this.encoding = encoding;
    }
    public int getThreads() {
        return nthreads;
    }
    public void setThreads(int nthreads) {
        this.nthreads = nthreads;
    }
    public long getCommitFrequency() {
        return commitFrequency;
    }
    public void setCommitFrequency(long commitFrequency) {
        this.commitFrequency = commitFrequency;
    }
    public int getMaxRetries() {
        return maxRetries;
    }
    public void setMaxRetries(int maxRetries) {
        this.maxRetries = maxRetries;
    }
    public boolean isQuiet() {
        return quiet;
    }
    public void setQuiet(boolean quiet) {
        this.quiet = quiet;
    }
    public String getConstraintCheckTime() {
        return constraintCheckTime;
    }
    public void setConstraintCheckTime(String constraintCheckTime) {
        this.constraintCheckTime = constraintCheckTime;
    }

    public long load(File file) throws Exception {
        FileInputStream stream = new FileInputStream(file);
        try {
            FileChannel channel = stream.getChannel();
            String target = this.target;
            if (target == null) {
                target = file.getName();
                int idx = target.lastIndexOf('.');
                if (idx >= 0)
                    target = target.substring(0, idx);
            }
            Format format = this.format;
            if (format == Format.AUTO) {
                String name = file.getName();
                if (name.endsWith(".csv")) {
                    format = Format.CSV;
                }
                else if (name.endsWith(".sql")) {
                    MySQLLoader loader = new MySQLLoader(this, channel, target);
                    if (loader.isMySQLDump())
                        format = Format.MYSQL_DUMP;
                    else
                        format = Format.FDB_SQL;
                }
                else {
                    throw new Exception("Cannot determine format for " + file + 
                                        ". Use --format explicitly.");
                }
            }
            FileLoader loader = null;
            switch (format) {
            case CSV:
            case CSV_HEADER:
                loader = new CsvLoader(this, channel, 
                                       target, (format == Format.CSV_HEADER));
                break;
            case MYSQL_DUMP:
                loader = new MySQLLoader(this, channel, target);
                break;
            case FDB_SQL:
                loader = new DumpLoader(this, channel);
                break;
            default:
                assert false : format;
            }
            try {
                loader.checkFormat();
            }
            catch (UnsupportedOperationException ex) {
                System.err.println(ex.getMessage());
                return -1;
            }
            long startTime = System.currentTimeMillis();
            if (!quiet) {
                System.out.println("Loading " + format.name + " file " + file + "...");
            }
            List<? extends SegmentLoader> segments;
            if (nthreads == 1)
                segments = Collections.singletonList(loader.wholeFile());
            else
                segments = loader.split(nthreads);
            for (SegmentLoader segment : segments) {
                segment.prepare();
            }
            if (segments.size() == 1) {
                segments.get(0).run();
            }
            else {
                Thread[] threads = new Thread[segments.size()];
                for (int i = 0; i < threads.length; i++) {
                    threads[i] = new Thread(segments.get(i));
                }
                for (int i = 0; i < threads.length; i++) {
                    threads[i].start();
                }
                for (int i = 0; i < threads.length; i++) {
                    threads[i].join();
                }
            }
            long endTime = System.currentTimeMillis();
            long total = 0;
            for (SegmentLoader segment : segments) {
                total += segment.count;
            }
            if (!quiet) {
                System.out.println("... loaded " + total + " rows in " +
                                   (endTime - startTime) / 1.0e3 + " s.");
            }
            return total;
        }
        finally {
            stream.close();
        }
    }

    protected Connection getConnection(boolean autoCommit) throws SQLException {
        Connection connection = connections.poll();
        if (connection == null) {
            String url = String.format("jdbc:postgresql://%s:%d/%s", host, port, schema);
            connection = DriverManager.getConnection(url, user, password);
        }
        connection.setAutoCommit(autoCommit);
        Statement stmt = null;
        if (commitFrequency == COMMIT_AUTO) {
            stmt = connection.createStatement();
            stmt.execute("SET transactionPeriodicallyCommit TO 'true'");
        }
        if (commitFrequency != 0) {
            if (stmt == null)
                stmt = connection.createStatement();
            stmt.execute("SET constraintCheckTime TO '" + constraintCheckTime + "'");
        }
        if (stmt != null)
            stmt.close();
        if (!autoCommit)
            connection.rollback();
        return connection;
    }

    protected void returnConnection(Connection connection) throws SQLException {
        if (!connections.offer(connection))
            connection.close();
    }

    protected void clearConnections() throws SQLException {
        while (true) {
            Connection connection = connections.poll();
            if (connection == null) break;
            connection.close();
        }
    }

    protected CopyManager getCopyManager(Connection connection) throws SQLException {
        return new CopyManager((org.postgresql.core.BaseConnection)connection);
    }
}
