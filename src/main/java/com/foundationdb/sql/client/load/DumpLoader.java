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

import java.io.IOException;
import java.nio.channels.FileChannel;
import java.sql.Connection;
import java.sql.Statement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

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
            throw new UnsupportedOperationException("File contains " + line + " and cannot be fast loaded. Try psql -f instead.");
        }
    }

    protected class DumpSegmentLoader extends SegmentLoader {
        public DumpSegmentLoader(long start, long end) {
            super(DumpLoader.this.client, DumpLoader.this.channel,
                  start, end);
        }

        @Override
        public void prepare() throws IOException {
            // TODO: Here we load an initial subsegment synchronously using
            // executeSegment, so that other segments can use its tables.
        }

        @Override
        public void run() {
            try {
                count += executeSegment(start, end);
            }
            catch (Exception ex) {
                ex.printStackTrace();
            }
        }
    }

    protected long executeSegment(long start, long end) 
            throws SQLException, IOException {
        LineReader lines = new LineReader(channel, client.getEncoding(), 
                                          BUFFER_SIZE, BUFFER_SIZE, 
                                          start, end);
        StringBuilder str = new StringBuilder();
        Connection conn = getConnection(hasDDL);
        Statement stmt = conn.createStatement();
        long count = 0;
        int pending = 0;
        boolean success = false;
        try {
            top: while (true) {
                str.setLength(0);
                while (true) {
                    if (!lines.readLine(str)) {
                        if (str.length() == 0) break top;
                        else break;
                    }
                    if (str.length() > 0) {
                        if (str.charAt(0) == '-') {
                            continue top;
                        }
                        if (str.charAt(str.length() - 1) == ';')
                            break;
                        str.append('\n');
                    }
                }
                String sql = str.toString();
                if (sql.startsWith("INSERT INTO ")) {
                    if (hasDDL && conn.getAutoCommit()) {
                        conn.setAutoCommit(false);
                    }
                    pending += stmt.executeUpdate(sql);
                    if ((client.getCommitFrequency() > 0) &&
                        (pending >= client.getCommitFrequency())) {
                        conn.commit();
                        count += pending;
                        pending = 0;
                    }
                }
                else {
                    if (pending > 0) {
                        conn.commit();
                        count += pending;
                        pending = 0;
                    }
                    conn.setAutoCommit(true);
                    hasDDL = true; // Just in case.
                    stmt.execute(sql);
                }
            }
            if (pending > 0) {
                conn.commit();
                count += pending;
            }
            success = true;
        }
        finally {
            stmt.close();
            returnConnection(conn, success);
        }
        return count;
    }

    public SegmentLoader wholeFile() throws IOException {
        long start = 0;
        long end = channel.size();
        return new DumpSegmentLoader(start, end);
    }

    public List<? extends SegmentLoader> split(int nsegments) throws IOException {
        List<DumpSegmentLoader> segments = new ArrayList<>(nsegments);
        long start = 0;
        long end = channel.size();
        LineReader lines = new LineReader(channel, client.getEncoding(),
                                          FileLoader.SMALL_BUFFER_SIZE, 1,
                                          start, end);
        while (nsegments > 1) {
            long mid = start + (end - start) / nsegments;
            mid = lines.newLineNear(mid, ';');
            if ((mid <= start) || (mid >= end)) break; // Not enough lines.
            segments.add(new DumpSegmentLoader(start, mid));
            start = mid;
            lines.position(mid);
            nsegments--;
        }
        segments.add(new DumpSegmentLoader(start, end));
        return segments;
    }
}
