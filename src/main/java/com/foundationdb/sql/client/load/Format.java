/**
 * Copyright (C) 2012-2014 FoundationDB, LLC
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

public enum Format
{
    AUTO("auto"), CSV("CSV"), CSV_HEADER("CSV with header"), MYSQL_DUMP("MySQL"), FDB_SQL("SQL");

    final String name;
    Format(String name) {
        this.name = name;
    }

    @Override
    public String toString() {
        return name;
    }

    public static Format fromName(String name) {
        for (Format fmt : values()) {
            if (name.equalsIgnoreCase(fmt.name)) {
                return fmt;
            }
        }
        throw new IllegalArgumentException("Unknown format: " + name);
    }
}
