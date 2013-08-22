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

abstract class SegmentLoader implements Runnable
{
    protected final LoadClient client;
    protected final FileChannel channel;
    protected long start, end, count;

    protected SegmentLoader(LoadClient client, FileChannel channel, long start, long end) {
        this.client = client;
        this.channel = channel;
        this.start = start;
        this.end = end;
    }

    /** This is called in the main thread before spawning multiple
     * per-segment threads, so it should only do the minimum amount of
     * work, such as <code>CREATE TABLE</code> DDL that might be used
     * by other segments.
     */
    public void prepare() throws IOException {
    }

}
