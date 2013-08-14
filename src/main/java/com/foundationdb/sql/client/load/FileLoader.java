/**
 * Copyright (C) 2012 Akiban Technologies Inc.
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
import java.sql.SQLException;
import java.util.List;

abstract class FileLoader
{
    protected static final int SMALL_BUFFER_SIZE = 1024;
    protected static final int BUFFER_SIZE = 65536;;

    protected final LoadClient client;
    protected final FileChannel channel;

    protected FileLoader(LoadClient client, FileChannel channel) {
        this.client = client;
        this.channel = channel;
    }

    public void checkFormat() throws IOException {
    }

    abstract SegmentLoader wholeFile() throws IOException;

    abstract List<SegmentLoader> split(int nsegments) throws IOException;

    protected Connection getConnection(boolean autoCommit) throws SQLException {
        return client.getConnection(autoCommit);
    }

    protected void returnConnection(Connection connection, boolean success) 
            throws SQLException {
        if (success)
            client.returnConnection(connection);
        else
            connection.close();
    }
}
