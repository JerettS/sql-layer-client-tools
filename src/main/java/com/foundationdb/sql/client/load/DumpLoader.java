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
import java.sql.Statement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import com.foundationdb.sql.client.cli.QueryBuffer;

class DumpLoader extends FileLoader
{
    boolean hasDDL;

    public DumpLoader(LoadClient client, FileChannel channel) {
        super(client, channel);
    }

    @Override
    public void checkFormat() throws IOException {
        LineReader lines = new LineReader(channel, client.getEncoding());
        while (true) {
            String line = lines.readLine();
            if (line == null) break;
            if (line.toUpperCase().startsWith("INSERT INTO "))
                return;         // Good.
            if (line.startsWith("DROP ")) {
                if (client.getThreads() > 1)
                    // TODO: This is actually possible, but when splitting you have to
                    // check for an INSERT whose CREATE TABLE hasn't been seen.  And then
                    // binary search back for it and start the segment there.
                    throw new UnsupportedOperationException("File contains DDL and cannot be loaded using multiple threads. Use fdbsqldump --no-schemas.");
                hasDDL = true;
                break;
            }
            if (line.isEmpty() || line.startsWith("--- "))
                continue;
            throw new UnsupportedOperationException("File contains " + line + " and cannot be fast loaded. Try fdbsqlcli -f instead.");
        }
    }

    protected class DumpSegmentQueryLoader extends SegmentLoader {
        public DumpSegmentQueryLoader(long start, long end) {
            super(DumpLoader.this.client, DumpLoader.this.channel, start, end);
        }
        
        @Override
        public void prepare() throws IOException {
        }
        
        @Override
        public void run() {
            try {
                count += executeSegmentQuery (start, end);
            } catch (Exception ex) {
                //TODO: Handle this better?
                ex.printStackTrace();
                
            }
        }
    }

    protected long executeSegmentQuery (long start, long end)
            throws SQLException, IOException {
        LineReader lines = new LineReader(channel, client.getEncoding(), 
                BUFFER_SIZE, BUFFER_SIZE, 
                start, end);
        List<String> uncommittedStatements = new ArrayList<String>();
        QueryBuffer buffer = new QueryBuffer ();
        Connection conn = getConnection(hasDDL);
        StatementHelper stmt = new StatementHelper(conn);
        CommitStatus status = new CommitStatus();
        boolean success = false;
        try {
            while (true) {
                if(!buffer.hasNonSpace()) {
                    buffer.reset();
                }
                if (lines.readLine(buffer)) {
                    while (buffer.hasQuery()) {
                        try {
                            String sql = buffer.nextQuery();
                            uncommittedStatements.add(sql);
                            executeSQL (conn, stmt, sql, status);
                            if (status.pending == 0) uncommittedStatements.clear();
                        } catch (SQLException e) {
                            if (!conn.getAutoCommit()) conn.rollback();
                            if (StatementHelper.shouldRetry(e, client.getMaxRetries() > 0)) {
                                retry(conn, stmt, status, uncommittedStatements, e);
                            } else {
                                throw(e);
                            }
                        }
                    }
                    buffer.reset();
                } else {
                    break;
                }
            }
            if (status.pending > 0) {
                try {
                    conn.commit();
                    status.commit();
                } catch (SQLException e) {
                    if (!conn.getAutoCommit()) conn.rollback();
                    if (StatementHelper.shouldRetry(e, client.getMaxRetries() > 0)) {
                        retry(conn, stmt, status, uncommittedStatements, e);
                    } else {
                        throw(e);
                    }
                }
            }
        } finally {
            stmt.close();
            returnConnection(conn, success);
        }
        return status.count;
    }

    @Override
    void executeSQL (Connection conn, StatementHelper helper, String sql, CommitStatus status ) throws SQLException {
        if (sql.startsWith("INSERT INTO ")) {
            if (hasDDL && conn.getAutoCommit()) {
                conn.setAutoCommit(false);
            }
            status.pending += helper.executeUpdate(sql);
            if ((client.getCommitFrequency() > 0) &&
                (status.pending >= client.getCommitFrequency())) {
                conn.commit();
                status.commit();
            }
        }
        else {
            if (status.pending > 0) {
                conn.commit();
                status.commit();
            }
            conn.setAutoCommit(true);
            hasDDL = true; // Just in case.
            helper.execute(sql);
        }
    }
    
    public SegmentLoader wholeFile() throws IOException {
        long start = 0;
        long end = channel.size();
        
        return new DumpSegmentQueryLoader(start, end);
    }

    
    public List<? extends SegmentLoader> split (int nsegments) throws IOException {
        return splitParse (nsegments);
    }
    
    protected List<? extends SegmentLoader> splitParse (int nsegments) throws IOException {
        List<DumpSegmentQueryLoader> segments = new ArrayList<>(nsegments);
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
            mid = lines.splitParse (mid);
            segments.add(new DumpSegmentQueryLoader(start, mid));
            if (mid >= (end - 1))
                return segments;
            start = mid;
            lines.position(mid);
            nsegments--;
        }
        segments.add(new DumpSegmentQueryLoader(start, end));
        return segments;
    }
    
}
