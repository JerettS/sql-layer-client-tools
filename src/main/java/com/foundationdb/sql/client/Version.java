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

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class Version
{
    private static final String VERSION_PROPERTY_FILE = "fdbsqlclient_version.properties";
    private static final Properties VERSION_PROPERTIES;

    /** As in pom: x.y.z[-SNAPSHOT] */
    public static final String VERSION;
    /** As in pom, no snapshot: x.y.z */
    public static final String VERSION_SHORT;
    /** As in pom, with short hash: x.y.z[-SNAPSHOT]+shortHash */
    public static final String VERSION_LONG;

    static {
        VERSION_PROPERTIES = new Properties();
        try(InputStream stream = ClassLoader.getSystemResourceAsStream(VERSION_PROPERTY_FILE)) {
            VERSION_PROPERTIES.load(stream);
        } catch (IOException e) {
            System.err.printf("Couldn't read version resource file %s: %s\n", VERSION_PROPERTY_FILE, e.getMessage());
        }
        VERSION = VERSION_PROPERTIES.getProperty("version", "0.0.0");
        VERSION_SHORT = VERSION.replace("-SNAPSHOT", "");
        String shortHash = VERSION_PROPERTIES.getProperty("git_hash", "");
        if(shortHash.length() > 7) {
            shortHash = shortHash.substring(0, 7);
        }
        VERSION_LONG = VERSION + "+" + shortHash;
    }

    public static void printVerbose() throws IllegalAccessException {
        for(String k : VERSION_PROPERTIES.stringPropertyNames()) {
            System.out.printf("%s=%s\n", k, VERSION_PROPERTIES.getProperty(k));
        }
    }

    public static void printMinimal() {
        System.out.println(VERSION_LONG);
    }

    public static void main(String[] args) throws Exception {
        if(args.length > 0 && "-v".equals(args[0])) {
            printVerbose();
        } else {
            printMinimal();
        }
    }
}
