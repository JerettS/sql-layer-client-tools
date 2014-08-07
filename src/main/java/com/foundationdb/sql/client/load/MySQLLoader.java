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

class MySQLLoader extends FileLoader
{
    private final String target;

    public MySQLLoader(LoadClient client, FileChannel channel, String target) {
        super(client, channel);
        this.target = target;
    }

    /** Is this .sql file really a MySQL dump file? */
    public boolean isMySQLDump() throws IOException {
        LineReader lines = new LineReader(channel, client.getEncoding());
        String header = lines.readLine();
        return ((header != null) && header.startsWith("-- MySQL dump "));
    }

    @Override
    public void checkFormat() throws IOException {
        LineReader lines = new LineReader(channel, client.getEncoding());
        while (true) {
            String line = lines.readLine();
            if (line == null) break;
            if ((line.length() == 0) ||
                line.startsWith("--") ||
                line.startsWith("/*"))
                continue;
            if (line.startsWith("LOCK ") ||
                line.startsWith("INSERT INTO "))
                return;         // Good.
            throw new UnsupportedOperationException("File contains " + line + " and can only be loaded by MySQL. Try mysqldump --no-create-info.");
        }
    }


    public SegmentLoader wholeFile() throws IOException {
        long start = 0;
        long end = channel.size();
        return new MySQLSegmentLoader(start,end);
    }

    public List<? extends SegmentLoader> split(int nsegments) throws IOException {
        List<MySQLSegmentLoader> segments = new ArrayList<>(nsegments);
        long start = 0;
        long end = channel.size();
        LineReader lines = new LineReader(channel, client.getEncoding(),
                FileLoader.SMALL_BUFFER_SIZE, 1,
                start, end);
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
            segments.add(new MySQLSegmentLoader(start, mid));
            if (mid >= (end - 1))
                return segments;
            start = mid;
            lines.position(mid);
            nsegments--;
        }
        segments.add(new MySQLSegmentLoader(start, end));
        return segments;
    }

    protected class MySQLSegmentLoader extends SegmentLoader {
        public MySQLSegmentLoader(long start, long end) {
            super(MySQLLoader.this.client, MySQLLoader.this.channel, start, end);
        }

        @Override
        public void prepare() throws IOException {
        }

        @Override
        public void run() {
            boolean success = false;
            Connection connection = null;
            CommitStatus status = new CommitStatus();
            StatementHelper stmt = null;
            List<MySQLBuffer.Query> uncommittedStatements = new ArrayList<>();
            LineReader lines = null;
            try {
                 lines = new LineReader(channel, client.getEncoding(),
                         BUFFER_SIZE, BUFFER_SIZE,
                         start, end);
                 connection = client.getConnection(false);
                 stmt = new StatementHelper(connection);
                 MySQLBuffer buffer = new MySQLBuffer(client.getEncoding());
                 while (true) {
                     if (!lines.readLine(buffer)) {
                         break;
                     }
                     MySQLBuffer.Query query = buffer.nextQuery();
                     try {
                         uncommittedStatements.add(query);
                         status.pending += stmt.executeUpdatePrepared(query.getPreparedStatement(), query.getValues());
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
                           List<MySQLBuffer.Query> uncommittedStatements, SQLException e) throws SQLException {
            for (int i = 0; StatementHelper.shouldRetry(e, i < client.getMaxRetries()); i++) {
                status.pending = 0;
                try {
                    for (MySQLBuffer.Query query : uncommittedStatements) {
                        status.pending += stmt.executeUpdatePrepared(query.getPreparedStatement(), query.getValues());
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
