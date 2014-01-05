/**
 * Copyright (C) 2009-2013 FoundationDB, LLC
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

package com.foundationdb.sql.client.cli;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;

public interface InputSource extends Closeable
{
    /** Return any descriptive info about this source or {@code null} if none. */
    String getInfo();

    /** Sets prompt to be used until changed. */
    void setPrompt(String prompt);

    /** File to store history in. */
    void openHistory(File file) throws IOException;

    /** Add to history, if present. */
    void addHistory(String line);

    /** Returns next string or {@code null} on EOF. Throw PartialLineException on user-abort (e.g. ctrl-c). */
    String readLine() throws IOException, PartialLineException;

    /** Close any open resources. */
    void close();
}
