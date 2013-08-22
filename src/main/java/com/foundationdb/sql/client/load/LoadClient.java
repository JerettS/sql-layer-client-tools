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

import java.nio.channels.FileChannel;

import java.io.*;
import java.sql.*;
import java.util.*;
import java.util.concurrent.ConcurrentLinkedDeque;

import org.postgresql.copy.CopyManager;

public class LoadClient
{
    public static enum Format {
        AUTO("auto"), CSV("CSV"), CSV_HEADER("CSV with header"), MYSQL_DUMP("MySQL"), FDB_SQL("SQL");

        final String name;
        Format(String name) {
            this.name = name;
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
    protected static final int COMMIT_AUTO = -1;
    private String host = DEFAULT_HOST;
    private int port = DEFAULT_PORT;
    private String user = "system";
    private String password = "system";
    private String schema = System.getProperty("user.name");
    private String target = null;
    private Format format = Format.AUTO;
    private String encoding = "UTF-8";
    private int nthreads = 1;
    private long commitFrequency = 0;
    private int maxRetries = 1;
    private List<File> files = new ArrayList<>();
    private boolean quiet = false;
    private Deque<Connection> connections = new ConcurrentLinkedDeque<>();

    public static void main(String[] args) throws Exception {
        Class.forName(DRIVER_NAME);
        LoadClient loadClient = new LoadClient();
        loadClient.parseArgs(args);
        if (loadClient.getFiles().isEmpty()) {
            usage();
            System.out.println("No files given.");
            System.exit(1);
        }
        try {
            for (File file : loadClient.getFiles()) {
                loadClient.load(file);
            }
        }
        finally {
            loadClient.clearConnections();
        }
    }

    protected static void usage() {
        System.out.println("LoadClient [-h host] [-p port] [-u user] [-w password] [-s schema] [--into target_table] [-f {auto,csv,mysql,sql} ] [-n nthreads] [-c commit_frequency] [-q] files...");
    }

    protected void parseArgs(String[] args) throws Exception {
        int i = 0;
        while (i < args.length) {
            String arg = args[i++];
            if (arg.startsWith("-")) {
                if ("--help".equals(arg)) {
                    usage();
                    System.exit(0);
                }
                else if ("-h".equals(arg) || ("--host".equals(arg))) {
                    setHost(args[i++]);
                }
                else if ("-p".equals(arg) || ("--port".equals(arg))) {
                    setPort(Integer.parseInt(args[i++]));
                }
                else if ("-u".equals(arg) || ("--user".equals(arg))) {
                    setUser(args[i++]);
                }
                else if ("-w".equals(arg) || ("--password".equals(arg))) {
                    setPassword(args[i++]);
                }
                else if ("-s".equals(arg) || ("--schema".equals(arg))) {
                    setSchema(args[i++]);
                }
                else if ("-f".equals(arg) || ("--format".equals(arg))) {
                    setFormat(Format.fromName(args[i++]));
                }
                else if ("--header".equals(arg)) {
                    setFormat(Format.CSV_HEADER);
                }
                else if ("-t".equals(arg) || ("--into".equals(arg))) {
                    setTarget(args[i++]);
                }
                else if ("-n".equals(arg) || ("--threads".equals(arg))) {
                    setThreads(Integer.parseInt(args[i++]));
                }
                else if ("-c".equals(arg) || ("--commit".equals(arg))) {
                    arg = args[i++];
                    setCommitFrequency("auto".equals(arg) ? COMMIT_AUTO : Long.parseLong(arg));
                }
                else if ("-r".equals(arg) || ("--retry".equals(arg))) {
                    setMaxRetries(Integer.parseInt(args[i++]));
                }
                else if ("-q".equals(arg) || ("--quiet".equals(arg))) {
                    setQuiet(true);
                }
                else {
                    throw new Exception("Unknown switch: " + arg);
                }
            }
            else {
                addFile(arg);
            }
        }
    }

    public LoadClient() {
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
    public Collection<File> getFiles() {
        return files;
    }
    public void addFile(File file) {
        files.add(file);
    }
    public void addFile(String filename) {
        addFile(new File(filename));
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
        if (commitFrequency == COMMIT_AUTO) {
            Statement stmt = connection.createStatement();
            stmt.execute("SET transactionPeriodicallyCommit TO 'true'");
            stmt.close();
        }
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
