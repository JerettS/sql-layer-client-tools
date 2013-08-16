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
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.List;

class CsvLoader extends FileLoader
{
    private final String target;
    private final boolean header;

    public CsvLoader(LoadClient client, FileChannel channel, 
                     String target, boolean header) {
        super(client, channel);
        this.target = target;
        this.header = header;
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
        String headerLine = null;
        if (header) {
            LineReader lines = new LineReader(channel, client.getEncoding(), 1); // Need accurate position.
            headerLine = lines.readLine();
            start = lines.position();
        }
        StringBuilder sql = new StringBuilder();
        sql.append("COPY ").append(target);
        if ((headerLine != null) && (target.indexOf('(') < 0))
            sql.append("(").append(headerLine).append(")");
        sql.append(" FROM STDIN WITH (ENCODING '") .append(client.getEncoding())
           .append("', FORMAT CSV");
        // TODO: DELIMITER, QUOTE, ESCAPE, ...?
        if (client.getCommitFrequency() > 0)
            sql.append(", COMMIT ").append(client.getCommitFrequency());
        sql.append(")");
        return new CopyLoader(client, channel, sql.toString(), start, end);
    }
}
