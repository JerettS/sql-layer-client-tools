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

import com.foundationdb.sql.client.StatementHelper;

import java.io.IOException;
import java.lang.UnsupportedOperationException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

class CsvLoader extends FileLoader
{
    private final String targetTable;
    private final boolean header;

    public CsvLoader(LoadClient client, FileChannel channel, 
                     String targetTable, boolean header) {
        super(client, channel);
        this.targetTable = targetTable;
        this.header = header;
    }

    public SegmentLoader wholeFile() throws IOException {
        long start = 0;
        long end = channel.size();
        String headerLine = null;
        if (header) {
            LineReader lines = new LineReader(channel, client.getEncoding(), 1); // Need accurate position.
            headerLine = lines.readLine();
            start = lines.position();
        }
        return new CsvSegmentLoader(start,end);
    }

    public List<? extends SegmentLoader> split(int nsegments) throws IOException {
        throw new UnsupportedOperationException();
    }

    protected CopyLoader getCopyLoader() throws IOException {
        long start = 0;
        long end = channel.size();
        String headerLine = null;
        if (header) {
            LineReader lines = new LineReader(channel, client.getEncoding(), 1); // Need accurate position.
            headerLine = lines.readLine();
            start = lines.position();
        }
        StringBuilder sql = new StringBuilder();
        sql.append("COPY ").append(targetTable);
        if ((headerLine != null) && (targetTable.indexOf('(') < 0))
            sql.append("(").append(headerLine).append(")");
        sql.append(" FROM STDIN WITH (ENCODING '") .append(client.getEncoding())
           .append("', FORMAT CSV");
        // TODO: DELIMITER, QUOTE, ESCAPE, ...?
        if (client.getCommitFrequency() > 0)
            sql.append(", COMMIT ").append(client.getCommitFrequency());
        if (client.getMaxRetries() > 1)
            sql.append(", RETRY ").append(client.getMaxRetries());
        sql.append(")");
        return new CopyLoader(client, channel, sql.toString(), start, end);
    }

    @Override
    void executeSQL (Connection conn, StatementHelper helper, String sql, CommitStatus status ) throws SQLException {
        if (sql.startsWith("INSERT INTO ")) {
            status.pending += helper.executeUpdate(sql);
            if ((client.getCommitFrequency() > 0) &&
                (status.pending >= client.getCommitFrequency())) {
                conn.commit();
                status.commit();
            }
        } else {
            throw new UnsupportedOperationException("CSV should only be inserting");
        }
    }

    protected class CsvSegmentLoader extends SegmentLoader {
        public CsvSegmentLoader(long start, long end) {
            super(CsvLoader.this.client, CsvLoader.this.channel, start, end);
        }

        @Override
        public void prepare() throws IOException {
        }

        @Override
        public void run(){
            boolean success = false;
            Connection connection = null;
            CommitStatus status = new CommitStatus();
            StatementHelper stmt = null;
            List<String> uncommittedStatements = new ArrayList<String>();
            LineReader lines = null;
            try {
                lines = new LineReader(channel, client.getEncoding(),
                                       BUFFER_SIZE, BUFFER_SIZE,
                                       start, end);
                connection = client.getConnection(false);
                stmt = new StatementHelper(connection);
                while (true) {
                    // TODO: what about newlines in quotes...
                    String csvLine = lines.readLine();
                    if (csvLine == null) {
                        break;
                    }
                    // TODO prepare
                    // TODO types?
                    try {
                        String sql = "INSERT INTO \"" + targetTable + "\" VALUES (" + csvLine + ")";
                        uncommittedStatements.add(sql);
                        executeSQL(connection, stmt, sql, status);
                        if (status.pending == 0) { // successful commit
                            uncommittedStatements.clear();
                        }
                    } catch (SQLException e) {
                        if (!connection.getAutoCommit()) connection.rollback();
                        if (StatementHelper.shouldRetry(e, client.getMaxRetries() > 0)) {
                            retry(connection, stmt, status, uncommittedStatements, e);
                        } else {
                            throw(e);
                        }
                    }

                }
                if (status.pending > 0) {
                    try {
                        connection.commit();
                        status.commit();
                    } catch (SQLException e) {
                        if (!connection.getAutoCommit()) connection.rollback();
                        if (StatementHelper.shouldRetry(e, client.getMaxRetries() > 0)) {
                            retry(connection, stmt, status, uncommittedStatements, e);
                        } else {
                            throw(e);
                        }
                    }
                }
                success = true;
            }
            catch (Exception ex) {
                ex.printStackTrace();
            }
            finally {
                if (stmt != null) {
                    stmt.close();
                }
                try {
                    returnConnection(connection, success);
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
        }
    }
}
