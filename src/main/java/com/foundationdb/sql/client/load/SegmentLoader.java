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
import java.sql.SQLException;

abstract class SegmentLoader implements Runnable
{
    protected final LoadClient client;
    protected final FileChannel channel;
    protected final long start;
    protected final long end;
    protected final long startLineNo;
    protected long count;

    protected SegmentLoader(LoadClient client, FileChannel channel, long start, long end, long startLineNo) {
        this.client = client;
        this.channel = channel;
        this.start = start;
        this.end = end;
        this.startLineNo = startLineNo;
    }

    /** This is called in the main thread before spawning multiple
     * per-segment threads, so it should only do the minimum amount of
     * work, such as <code>CREATE TABLE</code> DDL that might be used
     * by other segments.
     */
    public void prepare() throws IOException {
    }

    protected abstract void runSegment() throws SQLException, IOException, DumpLoaderException;

    @Override
    public final void run() {
        try {
            runSegment();
        } catch (Exception ex) {
            if (ex instanceof DumpLoaderException) {
                System.err.println("ERROR: During query that ends on line " +
                        ((DumpLoaderException) ex).getLineNo() + ", starting with:");
                System.err.println("       " + getPartialQuery(((DumpLoaderException) ex).getQuery(), 160));
                ex = ((DumpLoaderException) ex).getEx();
            }
            System.err.println(ex.getMessage());
            if (ex instanceof SQLException) {
                // unwrap SQLException
                if (ex.getCause() instanceof SQLException) {
                    ex = (SQLException) ex.getCause();
                    System.err.println(ex.getMessage());
                }

                if (StatementHelper.shouldRetry((SQLException) ex, true)) {
                    System.err.println("NOTE: In case of past version exception try flags: --commit=auto --retry=3");
                }
                System.err.println("NOTE: You can drop the partially loaded schema by doing: fdbsqlcli -c " +
                        "\"DROP SCHEMA [schema_name] CASCADE\"");
            }
        }
    }

    protected String getPartialQuery(String query, int maxLength){
        return query.length() > maxLength ? (query.substring(0, maxLength) + " ...") : query;
    }
}
