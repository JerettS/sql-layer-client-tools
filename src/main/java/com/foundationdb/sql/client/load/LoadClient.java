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

import com.foundationdb.sql.jdbc.copy.CopyManager;

import java.io.*;
import java.nio.channels.FileChannel;
import java.sql.*;
import java.util.*;
import java.util.concurrent.ConcurrentLinkedDeque;

public class LoadClient
{
    private static final String PROGRAM_NAME = "fdbsqlload";
    private static final String encoding = "UTF-8";

    private final LoadClientOptions options;
    private final Deque<Connection> connections = new ConcurrentLinkedDeque<>();
    private final List<String> urls;
    private int urlsIterator = 0;


    public static void main(String[] args) throws Exception {
        LoadClientOptions options = new LoadClientOptions();
        options.parseOrDie(PROGRAM_NAME, args);
        LoadClient loadClient = new LoadClient(options);
        try {
            for (File file : options.files) {
                loadClient.load(file);
            }
        } catch (Exception e) {
            System.err.println (e.getMessage());
            System.exit(1);
        }
        finally {
            loadClient.clearConnections();
        }
    }


    public LoadClient(LoadClientOptions options) {
        this.options = options;
        urls = options.getAllURLs();
        assert !urls.isEmpty() : "No connection URLs";
        if(options.commitFrequency == null) {
            options.commitFrequency = 0L;
        }
        if(options.maxRetries == null) {
            if (options.commitFrequency == 0) {
                options.maxRetries = 1;
            }
            else {
                options.maxRetries = 10;
            }
        }
    }

    public String getEncoding() {
        return encoding;
    }

    public long getThreads() {
        return options.nthreads;
    }

    public long getCommitFrequency() {
        return options.commitFrequency;
    }

    public long getMaxRetries() {
        return options.maxRetries;
    }

    public long load(File file) throws Exception {
        FileInputStream stream = new FileInputStream(file);
        try {
            FileChannel channel = stream.getChannel();
            String target = options.target;
            if (target == null) {
                target = file.getName();
                int idx = target.lastIndexOf('.');
                if (idx >= 0)
                    target = target.substring(0, idx);
            }
            Format format = options.format;
            if (format == Format.AUTO) {
                String name = file.getName();
                if (name.endsWith(".csv")) {
                    format = Format.CSV;
                }
                else if (name.endsWith(".sql")) {
                    MySQLLoader loader = new MySQLLoader(this, channel);
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
                if (options.target != null) {
                    System.err.println("MySQL import does not support the --into option");
                    return -1;
                }
                loader = new MySQLLoader(this, channel);
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
            if (!options.quiet) {
                System.out.println("Loading " + format.name + " file " + file + "...");
            }
            List<? extends SegmentLoader> segments;
            if (options.nthreads == 1)
                segments = Collections.singletonList(loader.wholeFile());
            else
                segments = loader.split(options.nthreads);
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
            if (!options.quiet) {
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
            final String url;
            synchronized(urls) {
                url = urls.get(urlsIterator++);
                urlsIterator %= urls.size();
            }
            connection = DriverManager.getConnection(url, options.user, options.password);
        }
        connection.setAutoCommit(autoCommit);
        Statement stmt = null;
        if (options.commitFrequency == LoadClientOptions.COMMIT_AUTO) {
            stmt = connection.createStatement();
            stmt.execute("SET transactionPeriodicallyCommit TO 'true'");
        }
        if (options.commitFrequency != 0) {
            if (stmt == null)
                stmt = connection.createStatement();
            stmt.execute("SET constraintCheckTime TO '" + options.constraintCheckTime + "'");
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
        
        for (Connection connection : connections) {
            connection.close();
        }
    }

    protected CopyManager getCopyManager(Connection connection) throws SQLException {
        return new CopyManager((com.foundationdb.sql.jdbc.core.BaseConnection)connection);
    }
}
