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
import java.nio.CharBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.Charset;
import java.nio.charset.CharsetDecoder;

import com.foundationdb.sql.client.cli.QueryBuffer;

public class LineReader
{
    public static final int SHORT_LINE = 128;

    private final FileChannel channel;
    private final CharsetDecoder decoder;
    private final ByteBuffer bytes;
    private final CharBuffer chars;
    private long position, limit;
    private long lineCounter;

    public LineReader(FileChannel channel, String encoding) throws IOException {
        this(channel, encoding, SHORT_LINE);
    }

    /** Use this with <code>charSize = 1</code> if ending position
     * needs to be accurate after each line. It will be slower because
     * it can only decode one character at a time.
     */
    public LineReader(FileChannel channel, String encoding, int charSize) throws IOException {
        this(channel, encoding, FileLoader.SMALL_BUFFER_SIZE, charSize, 0, channel.size());
    }

    public LineReader(FileChannel channel, String encoding, 
                      int byteSize, int charSize,
                      long position, long limit)
            throws IOException {
        this.channel = channel;
        this.decoder = Charset.forName(encoding).newDecoder();
        this.bytes = ByteBuffer.allocate(byteSize);
        this.chars = CharBuffer.allocate(charSize);
        chars.flip();           // Normal state is bytes filling, chars emptying.
        this.position = position;
        this.limit = limit;
        this.lineCounter = 0;
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

    public void resetLineCounter(){
        this.lineCounter = 0;
    }
    
    public long getLineCounter() {
        return this.lineCounter;
    }
    
    public String readLine() throws IOException {
        StringBuilder str = new StringBuilder();
        if (!readLine(str) && (str.length() == 0))
            return null;
        return str.toString();
    }
    
    public boolean readLine (QueryBuffer into) throws IOException {
        into.setStripDashQuote();
        StringBuilder line = new StringBuilder (SHORT_LINE);
        boolean eol = false;
        while (true) {
            while (chars.hasRemaining()) {
                char ch = chars.get();
                if (ch == '\n') {
                    eol = true;
                    lineCounter++;
                    break;
                }
                else if (ch != '\r')
                    line.append(ch);
            }
            
            if (eol) {
                if(!into.isEmpty()) {
                    into.append('\n'); // replace the \n
                    into.append(line.toString());
                } else {
                    into.append(line.toString());
                }
                line.setLength(0);
                eol = false;
                if (into.hasQuery()) return true;
            } else {
                if (!refillCharsBuffer()) {
                    return false;
                }
            }
        }
    }

    // TODO can this be combined with QueryBuffer or maybe even CsvBuffer with an interface?
    public boolean readLine (MySQLBuffer into) throws IOException,MySQLBuffer.ParseException {
        StringBuilder line = new StringBuilder (SHORT_LINE);
        boolean eol = false;
        while (true) {
            while (chars.hasRemaining()) {
                char ch = chars.get();
                if (ch == '\n') {
                    eol = true;
                    lineCounter++;
                    break;
                }
                else if (ch != '\r')
                    line.append(ch);
            }

            if (eol) {
                if(!into.isEmpty()) {
                    into.append('\n'); // replace the \n
                    into.append(line.toString());
                } else {
                    into.append(line.toString());
                }
                line.setLength(0);
                eol = false;
                if (into.hasQuery()) return true;
            } else {
                if (!refillCharsBuffer()) {
                    return false;
                }
            }
        }
    }

    public boolean readLine (CsvBuffer into) throws IOException {
        boolean eol = false;
        while (true) {
            while (chars.hasRemaining()) {
                char ch = chars.get();
                into.append(ch);
                if (ch == '\n') {
                    eol = true;
                    break;
                }
            }

            if (eol) {
                eol = false;
                if (into.hasRow()) return true;
            } else {
                if (!refillCharsBuffer()) {
                    if (!into.isEmpty()) {
                        into.append('\n'); // replace the \n
                        return into.hasRow();
                    }
                    return false;
                }
            }
        }
    }

    public boolean readLine(StringBuilder into) throws IOException {
        while (true) {
            while (chars.hasRemaining()) {
                char ch = chars.get();
                if (ch == '\n') {
                    lineCounter++;
                    return true;
                }
                else if (ch != '\r')
                    into.append(ch);
            }
            if (!refillCharsBuffer()) {
                return false;
            }
        }
    }

    private boolean refillCharsBuffer() throws IOException {
        long startPosition = position;
        if (position >= limit) return false;
        chars.clear();
        if (bytes.hasRemaining()) {
            if (position + bytes.limit() > limit)
                bytes.limit((int)(limit - position));
            channel.read(bytes, position + bytes.position());
        }
        bytes.flip();
        decoder.decode(bytes, chars, (position + bytes.limit() >= limit));
        position += bytes.position();
        
        // This case occurs when, due to a bug, the limit 
        // is beyond the end of the channel. This becomes an infinite 
        // loop.
        assert position != startPosition : "End position beyond the the end of the channel.";
        bytes.compact();
        chars.flip();
        return true;
    }

    public long newLineNear(long point) throws IOException {
        return newLineNear(point, -1);
    }

    public long splitParse (long point) throws IOException {
        QueryBuffer b  = new QueryBuffer();
        long before = -1;
        long after = -1;
        decoder.reset();
        
        while (position < point) {
            before = position;
            readLine(b);
            b.reset();
            after = position;
        }
        
        if (before < after) {
            return after;
        } else {
            return before;
        }
    }

    public long splitParseCsv (long point, CsvBuffer buffer) throws IOException {
        long before = -1;
        long after = -1;
        decoder.reset();

        while (position < point) {
            before = position;
            readLine(buffer);
            buffer.reset();
            after = position;
        }

        if (before < after) {
            return after;
        } else {
            return before;
        }
    }
    
    
    public long newLineNear(long point, int prevChar) throws IOException {
        // NB: ints are relative positions, longs are absolute.
        // Find nearest in region around given point.
        long regionStart = point - bytes.capacity() / 2;
        if (regionStart < position) regionStart = position;
        long regionEnd = regionStart + bytes.capacity();
        if (regionEnd > limit) regionEnd = limit;
        bytes.clear();
        bytes.limit((int)(regionEnd - regionStart));
        channel.read(bytes, regionStart);
        bytes.flip();
        assert (bytes.limit() == (regionEnd - regionStart));
        {
            int after = bytes.limit() / 2;
            int before = bytes.limit() - after - 1;
            while (before >= 0) {
                if ((bytes.get(before) == '\n') &&
                    ((prevChar < 0) ||
                     ((before > 0) && (bytes.get(before-1) == prevChar))))
                    return regionStart + before + 1;
                if ((after != before) &&
                    (bytes.get(after) == '\n') &&
                    ((prevChar < 0) ||
                     (bytes.get(after-1) == prevChar)))
                    return regionStart + after + 1;
                after++;
                before--;
            }
        }
        // Didn't find in that region. Check whole buffers either side.
        int size = bytes.capacity();
        if (prevChar >= 0) size--; // Some overlap.
        long before = -1;
        before: while (regionStart > position) {
            regionStart -= size;
            if (regionStart < position) regionStart = position;
            long beforeEnd = regionStart + bytes.capacity();
            if (beforeEnd > limit) beforeEnd = limit;
            bytes.clear();
            bytes.limit((int)(beforeEnd - regionStart));
            channel.read(bytes, regionStart);
            bytes.flip();
            assert (bytes.limit() == (beforeEnd - regionStart));
            for (int i = bytes.limit() - 1; i >= 0; i--) {
                if ((bytes.get(i) == '\n') &&
                    ((prevChar < 0) ||
                     ((i > 0) && (bytes.get(i-1) == prevChar)))) {
                    before = regionStart + i + 1;
                    break before;
                }
            }
        }
        long after = -1;
        after: while (regionEnd < limit) {
            regionEnd += size;
            if (regionEnd > limit) regionEnd = limit;
            long afterStart = regionEnd - bytes.capacity();
            if (afterStart < position) afterStart = position;
            bytes.clear();
            bytes.limit((int)(regionEnd - afterStart));
            channel.read(bytes, afterStart);
            bytes.flip();
            assert (bytes.limit() == (regionEnd - afterStart));
            for (int i = 0; i < bytes.limit(); i++) {
                if ((bytes.get(i) == '\n') &&
                    ((prevChar < 0) ||
                     ((i > 0) && (bytes.get(i-1) == prevChar)))) {
                    after = afterStart + i + 1;
                    break after;
                }
            }
        }
        if (before < 0)
            return after;
        else if ((point - before) < (after - point))
            return before;
        else
            return after;
    }

}
