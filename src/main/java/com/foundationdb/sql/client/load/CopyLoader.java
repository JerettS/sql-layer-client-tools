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
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import org.postgresql.copy.CopyIn;

class CopyLoader extends SegmentLoader
{
    protected final String sql;

    protected CopyLoader(LoadClient client, FileChannel channel, 
                         String sql, long start, long end) {
        super(client, channel, start, end);
        this.sql = sql;
    }

    public String getSQL() {
        return sql;
    }

    @Override
    public void run() {
        boolean success = false;
        Connection connection = null;
        CopyIn copy = null;
        try {
            connection = client.getConnection(true);
            copy = client.getCopyManager(connection).copyIn(sql);
            copySegment(copy);
            count = copy.endCopy();
            success = true;
        }
        catch (Exception ex) {
            ex.printStackTrace();
        }
        finally {
            if (copy != null) {
                if (copy.isActive()) {
                    try {
                        copy.cancelCopy();
                    }
                    catch (SQLException ex) {
                        ex.printStackTrace();
                    }
                }
            }
            if (connection != null) {
                try {
                    if (success)
                        client.returnConnection(connection);
                    else
                        connection.close();
                }
                catch (SQLException ex) {
                    ex.printStackTrace();
                }
            }
        }
    }

    /** Copy from <code>channel</code> to <code>copy</code> bytes from
     * <code>start</code> to <code>end</code>.
     */
    protected void copySegment(CopyIn copy) throws SQLException, IOException {
        ByteBuffer buffer = ByteBuffer.allocate(FileLoader.BUFFER_SIZE);
        long position = start;
        while (position < end) {
            buffer.clear();
            if (position + buffer.limit() > end) {
                buffer.limit((int)(end - position));
            }
            channel.read(buffer, position);
            copy.writeToCopy(buffer.array(), 0, buffer.position());
            position += buffer.position();
        }
    }

    /** Split this <code>CopyLoader</code> at the given position. */
    public CopyLoader split(long at) {
        assert ((at > start) && (at < end));
        CopyLoader second = new CopyLoader(client, channel, sql, at, end);
        end = at;
        return second;
    }

    public List<CopyLoader> splitByLines(int nsegments) throws IOException {
        List<CopyLoader> segments = new ArrayList<>(nsegments);
        segments.add(this);
        CopyLoader last = this;
        LineReader lines = new LineReader(channel, client.getEncoding(),
                                          FileLoader.SMALL_BUFFER_SIZE, 1,
                                          last.start, last.end);
        while (nsegments > 1) {
            long mid = last.start + (last.end - last.start) / nsegments;
            mid = lines.newLineNear(mid);
            if ((mid <= last.start) || (mid >= last.end)) break; // Not enough lines.
            last = last.split(mid);
            segments.add(last);
            lines.position(mid);
            nsegments--;
        }
        return segments;
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + "(" +
            sql + " from " + channel + "[" + start + ", " + end + "])";
    }

}
