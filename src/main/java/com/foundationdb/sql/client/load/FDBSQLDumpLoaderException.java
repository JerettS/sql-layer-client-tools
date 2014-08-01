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

public class FDBSQLDumpLoaderException extends RuntimeException {
    private Exception exception;
    private long lineNo;
    private String query;
    
    public FDBSQLDumpLoaderException(long lineNo, String query, Exception exception) {
        this.exception = exception;
        this.lineNo = lineNo;
        this.query = query == null ? "" : query;
    }

    public long getLineNo()  {
        return lineNo;
    }
    
    public String getQuery() {
        return query;
    }
    
    public Exception getEx() {
        return exception;
    }
}
