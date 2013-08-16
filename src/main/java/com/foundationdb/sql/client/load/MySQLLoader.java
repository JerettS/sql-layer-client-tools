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
import java.util.List;

class MySQLLoader extends FileLoader
{
    private final String target;

    public MySQLLoader(LoadClient client, FileChannel channel, String target) {
        super(client, channel);
        this.target = target;
    }

    public SegmentLoader wholeFile() throws IOException {
        return getCopyLoader();
    }

    public List<? extends SegmentLoader> split(int nsegments) throws IOException {
        return getCopyLoader().splitByLines(nsegments);
    }

    protected CopyLoader getCopyLoader() throws IOException {
        long start = 0;
        long end = channel.size();
        StringBuilder sql = new StringBuilder();
        sql.append("COPY ").append(target);
        sql.append(" FROM STDIN WITH (FORMAT MYSQL_DUMP");
        if (client.getCommitFrequency() > 0)
            sql.append(", COMMIT ").append(client.getCommitFrequency());
        sql.append(")");
        return new CopyLoader(client, channel, sql.toString(), start, end);
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
}
