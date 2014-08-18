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

import java.util.List;

public class StringUtils {
    public static String joinList(List list) {
        if (list == null) {
            return "null";
        }
        StringBuilder stringBuilder = new StringBuilder();
        stringBuilder.append('[');
        if (list.size() > 0) {
            stringBuilder.append(list.get(0));
            for (int i = 1; i < list.size(); i++) {
                stringBuilder.append(',');
                stringBuilder.append(list.get(i));
            }
        }
        stringBuilder.append(']');
        return stringBuilder.toString();
    }
}
