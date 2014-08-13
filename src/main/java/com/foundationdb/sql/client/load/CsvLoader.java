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
import java.nio.channels.FileChannel;
import java.sql.Connection;
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

    public SegmentLoader wholeFile() throws IOException, LineReader.ParseException {
        long start = 0;
        long end = channel.size();
        start = createPreparedStatement();
        return new CsvSegmentLoader(start,end);
    }

    private long createPreparedStatement() throws IOException, LineReader.ParseException {
        long start;List<String> columns = null;
        int columnCount = 0;
        LineReader lines = new LineReader(channel, client.getEncoding(), 1); // Need accurate position.
        CsvBuffer buffer = new CsvBuffer();
        if (lines.readLine(buffer) && buffer.hasStatement()) {
            if (header) {
                columns = buffer.nextStatement();
                columnCount = columns.size();
            } else {
                columnCount = buffer.nextStatement().size();
            }
        } else {
            throw new IndexOutOfBoundsException("Csv file is empty");
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
        sb.append(escapeIdentifier(targetTable));
        sb.append("\" ");
        if (columns != null) {
            assert columnCount == columns.size();
            sb.append("(\"");
            for (int i=0; i<columns.size()-1; i++) {
                sb.append(escapeIdentifier(columns.get(i)));
                sb.append("\",\"");
            }
            sb.append(escapeIdentifier(columns.get(columns.size()-1)));
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

    private static String escapeIdentifier(String identifier) {
        return identifier.replaceAll("\"","\"\"");
    }

    public List<? extends SegmentLoader> split(int nsegments) throws IOException, LineReader.ParseException {
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
            mid = lines.splitParse(mid, new CsvBuffer());
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
            List<String[]> uncommittedStatements = new ArrayList<>();
            LineReader lines = null;
            try {
                lines = new LineReader(channel, client.getEncoding(),
                                       BUFFER_SIZE, BUFFER_SIZE,
                                       start, end);
                connection = client.getConnection(false);
                stmt = new StatementHelper(connection);
                CsvBuffer buffer = new CsvBuffer();
                while (true) {
                    if (!lines.readLine(buffer)) {
                        break;
                    }
                    List<String> values = buffer.nextStatement();
                    try {
                        String[] valuesArray = values.toArray(new String[values.size()]);
                        uncommittedStatements.add(valuesArray);
                        status.pending += stmt.executeUpdatePrepared(preparedStatement, valuesArray);
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

        private void retry(Connection connection, StatementHelper stmt, CommitStatus status,
                           List<String[]> uncommittedStatements, SQLException e) throws SQLException {
            for (int i = 0; StatementHelper.shouldRetry(e, i < client.getMaxRetries()); i++) {
                status.pending = 0;
                try {
                    for (String[] values : uncommittedStatements) {
                        status.pending += stmt.executeUpdatePrepared(preparedStatement, values);
                    }
                    if (status.pending > 0) {
                        connection.commit();
                        status.commit();
                    }
                    uncommittedStatements.clear();
                    return;
                } catch (SQLException newE) {
                    if (!connection.getAutoCommit()) connection.rollback();
                    if (!StatementHelper.shouldRetry(newE, true)) {
                        throw(newE);
                    }
                    e = newE;
                }
            }
            throw(new SQLException("Maximum number of retries met", e));
        }
    }
}
