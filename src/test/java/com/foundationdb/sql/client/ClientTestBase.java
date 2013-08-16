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

import java.io.*;
import java.sql.*;
import java.util.*;
import java.util.regex.Pattern;

public abstract class ClientTestBase
{
    public static final String DRIVER_NAME = "org.postgresql.Driver";
    public static final String DEFAULT_HOST = "localhost";
    public static final int DEFAULT_PORT = 15432;
    public static final String SCHEMA_NAME = "dump_test";
    public static final String USER_NAME = "test";
    public static final String USER_PASSWORD = "test";

    protected Connection openConnection() throws Exception {
        String url = String.format("jdbc:postgresql://%s:%d/%s",
                                   DEFAULT_HOST, DEFAULT_PORT, SCHEMA_NAME);
        Class.forName(DRIVER_NAME);
        return DriverManager.getConnection(url, USER_NAME, USER_PASSWORD);
    }

    protected void dropSchema() throws Exception {
        openConnection().createStatement().execute("DROP SCHEMA IF EXISTS " + SCHEMA_NAME + " CASCADE");
    }

    public static File[] listMatchingFiles(File dir, String pattern) {
        File[] result = dir.listFiles(new RegexFilenameFilter(pattern));
        Arrays.sort(result, new Comparator<File>() {
                        public int compare(File f1, File f2) {
                            return f1.getName().compareTo(f2.getName());
                        }
                    });
        return result;
    }

    public static String fileContents(File file) throws IOException {
        FileReader reader = null;
        try {
            reader = new FileReader(file);
            StringBuilder str = new StringBuilder();
            char[] buf = new char[128];
            while (true) {
                int nc = reader.read(buf);
                if (nc < 0) break;
                str.append(buf, 0, nc);
            }
            return str.toString();
        }
        finally {
            if (reader != null) {
                try {
                    reader.close();
                }
                catch (IOException ex) {
                }
            }
        }
    }

    public static class RegexFilenameFilter implements FilenameFilter
    {
        Pattern pattern;

        public RegexFilenameFilter(String regex) {
            this.pattern = Pattern.compile(regex);
        }

        @Override
        public boolean accept(File dir, String name) {
            return pattern.matcher(name).matches();
        }
    }

}
