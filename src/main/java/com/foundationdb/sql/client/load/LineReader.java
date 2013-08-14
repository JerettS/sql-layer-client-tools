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
import java.nio.CharBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.Charset;
import java.nio.charset.CharsetDecoder;

public class LineReader
{
    public static final int SHORT_LINE = 128;

    private final FileChannel channel;
    private final CharsetDecoder decoder;
    private final ByteBuffer bytes;
    private final CharBuffer chars;
    private long position, limit;

    public LineReader(FileChannel channel) throws IOException {
        this(channel, "UTF-8");
    }

    /** Use this with <code>charSize = 1</code> if ending position
     * needs to be accurate after each line. It will be slower because
     * it can only decode one character at a time.
     */
    public LineReader(FileChannel channel, int charSize) throws IOException {
        this(channel, "UTF-8", FileLoader.SMALL_BUFFER_SIZE, charSize);
    }

    public LineReader(FileChannel channel, String encoding) throws IOException {
        this(channel, encoding, FileLoader.SMALL_BUFFER_SIZE, SHORT_LINE);
    }

    public LineReader(FileChannel channel, String encoding, int byteSize, int charSize) throws IOException {
        this.channel = channel;
        this.decoder = Charset.forName(encoding).newDecoder();
        this.bytes = ByteBuffer.allocate(byteSize);
        this.chars = CharBuffer.allocate(charSize);
        this.position = 0;
        this.limit = channel.size();
    }

    public long position() {
        return position;
    }

    public void position(long position) {
        this.position = position;
    }

    public long limit() {
        return limit;
    }

    public void limit(long limit) {
        this.limit = limit;
    }

    public String readLine() throws IOException {
        StringBuilder str = new StringBuilder();
        if (!readLine(str) && (str.length() == 0))
            return null;
        return str.toString();
    }

    public boolean readLine(StringBuilder into) throws IOException {
        while (position < limit) {
            while (chars.hasRemaining()) {
                char ch = chars.get();
                if (ch == '\n') return true;
                else if (ch != '\r')
                    into.append(ch);
            }
            chars.clear();
            if (bytes.hasRemaining()) {
                if (position + bytes.limit() > limit)
                    bytes.limit((int)(limit - position));
                channel.read(bytes, position + bytes.position());
            }
            bytes.flip();
            decoder.decode(bytes, chars, (position + bytes.limit() >= limit));
            position += bytes.position();
            bytes.compact();
            chars.flip();
        }
        return false;
    }

}
