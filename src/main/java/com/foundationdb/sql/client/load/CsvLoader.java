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
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

class CsvLoader extends FileLoader
{
    private final String targetTable;
    private final boolean header;
    private String preparedStatement;

    public CsvLoader(LoadClient client, FileChannel channel, 
                     String targetTable, boolean header) {
        super(client, channel);
        this.targetTable = targetTable;
        this.header = header;
    }

    public SegmentLoader wholeFile() throws IOException {
        long start = 0;
        long end = channel.size();
        start = createPreparedStatement();
        return new CsvSegmentLoader(start,end);
    }

    private long createPreparedStatement() throws IOException {
        long start;List<String> columns = null;
        int columnCount = 0;
        LineReader lines = new LineReader(channel, client.getEncoding(), 1); // Need accurate position.
        CsvBuffer buffer = new CsvBuffer(client.getEncoding());
        if (lines.readLine(buffer) && buffer.hasRow()) {
            if (header) {
                columns = buffer.nextRow();
                columnCount = columns.size();
            } else {
                columnCount = buffer.nextRow().size();
            }
        }
        if (header) {
            // since CsvBuffer will always end a row at the end of a line, the position must be the end of a line.
            // Well, so long as the character buffer size is 1 above
            start = lines.position();
        } else {
            start = 0;
        }
        preparedStatement = createPreparedStatement(targetTable, columns, columnCount);
        return start;
    }

    private static String createPreparedStatement(String targetTable, List<String> columns, int columnCount) {
        StringBuilder sb = new StringBuilder();
        sb.append("INSERT INTO \"");
        // TODO escape targetTable
        sb.append(targetTable);
        sb.append("\" ");
        // TODO throw exception if columnCount == 0
        // TODO and escape columns
        if (columns != null) {
            sb.append("(\"");
            for (int i=0; i<columns.size()-1; i++) {
                sb.append(columns.get(i));
                sb.append("\",\"");
            }
            sb.append(columns.get(columns.size()-1));
            sb.append("\") ");
        }
        sb.append("VALUES (");
        for (int i=0; i<columnCount-1; i++) {
            sb.append("?, ");
        }
        if (columnCount > 0) {
            sb.append("?)");
        } else {
            sb.append(")");
        }
        return sb.toString();
    }

    public List<? extends SegmentLoader> split(int nsegments) throws IOException {
        List<CsvSegmentLoader> segments = new ArrayList<>(nsegments);
        long start = 0;
        long end = channel.size();
        LineReader lines = new LineReader(channel, client.getEncoding(),
                                          FileLoader.SMALL_BUFFER_SIZE, 1,
                                          start, end);
        start = createPreparedStatement();
        long mid;
        while (nsegments > 1) {
            if ( ((end - start) < nsegments) && ((end - start) > 0)) {
                mid = start + 1;
                nsegments = (int)(end - start);
            }
            else {
                mid = start + (end - start) / nsegments;
            }
            mid = lines.splitParseCsv(mid, new CsvBuffer(client.getEncoding()));
            segments.add(new CsvSegmentLoader(start, mid));
            if (mid >= (end - 1))
                return segments;
            start = mid;
            lines.position(mid);
            nsegments--;
        }
        segments.add(new CsvSegmentLoader(start, end));
        return segments;
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
            String[] emptyStringArray = new String[0];
            try {
                lines = new LineReader(channel, client.getEncoding(),
                                       BUFFER_SIZE, BUFFER_SIZE,
                                       start, end);
                connection = client.getConnection(false);
                stmt = new StatementHelper(connection);
                CsvBuffer buffer = new CsvBuffer(client.getEncoding());
                while (true) {
                    if (!lines.readLine(buffer)) {
                        break;
                    }
                    List<String> values = buffer.nextRow();
                    // TODO prepare
                    // TODO types?
                    try {
                        String sql = "INSERT INTO \"" + targetTable + "\" VALUES (" + values + ")";
                        // TODO uncommitted statements need to escape or something
                        uncommittedStatements.add(sql);
                        status.pending += stmt.executeUpdatePrepared(preparedStatement, values.toArray(emptyStringArray));
//                        executeSQL(connection, stmt, sql, status);
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
            count += status.count;
        }
    }
}
