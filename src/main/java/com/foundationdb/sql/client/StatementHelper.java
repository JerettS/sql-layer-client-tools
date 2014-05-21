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

package com.foundationdb.sql.client;

import java.io.Closeable;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.IdentityHashMap;
import java.util.Map;

public class StatementHelper implements Closeable
{
    public final static String STALE_STATEMENT_CODE = "0A50A";
    public final static String ROLLBACK_PREFIX = "40";
    public final static boolean RETRY_ROLLBACK_DEFAULT = false;

    private final Connection conn;
    private final Map<String,PreparedStatement> preparedMap = new HashMap<String,PreparedStatement>();
    private Statement stmt;


    public StatementHelper(Connection conn) {
        this.conn = conn;
    }

    @Override
    public void close() {
        clearPreparedMap();
    }

    public void clearPreparedMap() {
        for(PreparedStatement ps : preparedMap.values()) {
            try {
                ps.close();
            } catch(SQLException e) {
                // Ignore
            }
        }
        preparedMap.clear();
    }


    public ResultSet executeQuery(String query) throws SQLException {
        return executeQuery(query, RETRY_ROLLBACK_DEFAULT);
    }

    public ResultSet executeQuery(String query, boolean retryRollback) throws SQLException {
        execute(query, retryRollback);
        return stmt.getResultSet();
    }

    public boolean execute(String query) throws SQLException {
        return execute(query, RETRY_ROLLBACK_DEFAULT);
    }

    public boolean execute(String query, boolean retryRollback) throws SQLException {
        for(;;) {
            try {
                if(stmt == null) {
                    stmt = conn.createStatement();
                }
                return stmt.execute(query);
            } catch(SQLException e) {
                if(!shouldRetry(e, retryRollback)) {
                    throw e;
                }
                // else retry
            }
        }
    }

    public ResultSet executeQueryPrepared(String query, String... args) throws SQLException {
        return executeQueryPrepared(query, RETRY_ROLLBACK_DEFAULT, args);
    }

    public ResultSet executeQueryPrepared(String query, boolean retryRollback, String... args) throws SQLException {
        for(;;) {
            PreparedStatement ps;
            try {
                ps = preparedMap.get(query);
                if(ps == null) {
                    ps = conn.prepareStatement(query);
                }
                for(int i = 0; i < args.length; ++i) {
                    ps.setString(i+1, args[i]);
                }
                return ps.executeQuery();
            } catch(SQLException e) {
                if(!shouldRetry(e, retryRollback)) {
                    throw e;
                }
                preparedMap.remove(query);
                // retry
            }
        }
    }

    private static boolean shouldRetry(SQLException e, boolean retryRollback) {
        return STALE_STATEMENT_CODE.equals(e.getSQLState()) ||
               (retryRollback && e.getSQLState().startsWith(ROLLBACK_PREFIX));
    }
}
