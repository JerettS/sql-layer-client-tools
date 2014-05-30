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

    /** Add to history, if present. Return a message to give to the user or null if none. */
    String addHistory(String input);

    /** Returns next string or {@code null} on EOF. Throw PartialLineException on user-abort (e.g. ctrl-c). */
    String readSome() throws IOException, PartialLineException;

    /** Close any open resources. */
    void close();
}
