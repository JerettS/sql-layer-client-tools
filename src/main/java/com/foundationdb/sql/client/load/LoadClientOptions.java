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

import com.beust.jcommander.IParameterValidator;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParameterException;
import com.beust.jcommander.Parameters;
import com.beust.jcommander.converters.BaseConverter;
import com.foundationdb.sql.client.ClientOptionsBase;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

@Parameters (separators = "=")
public class LoadClientOptions extends ClientOptionsBase
{
    public static final int COMMIT_AUTO = -1;

    public static class FormatConverter extends BaseConverter<Format>
    {
        public FormatConverter(String optionName) {
            super(optionName);
        }

        @Override
        public Format convert(String value) {
            return Format.fromName(value);
        }
    }

    public static class CommitConverter extends BaseConverter<Long>
    {
        public CommitConverter(String optionName) {
            super(optionName);
        }

        @Override
        public Long convert(String value) {
            return ("auto".equals(value) ? COMMIT_AUTO : Long.parseLong(value));
        }
    }

    public static class ConstraintCheckTimeValidator implements IParameterValidator
    {
        @Override
        public void validate(String name, String value) throws ParameterException {
            if (!value.matches("\\w+")) {
                throw new ParameterException("Parameter " + name + " is not a keyword");
            }
        }
    }

    public List<String> getAllURLs(){
        List<String> urls = new ArrayList<>();
        for(String h : hosts) {
            String url = formatURL(h, port, schema);
            urls.add(url);
        }
        return urls;
    }

    @Override
    public String getHost() {
        return hosts.get(0);
    }

    @Override
    public void setHost(String host) {
        this.hosts.clear();
        this.hosts.add(host);
    }

    @Parameter(names = { "-h", "--host" }, description = "server host, name or IP (multiple allowed)")
    public List<String> hosts = new ArrayList<String>(Collections.singleton(DEFAULT_HOST));

    @Parameter(names = { "-s", "--schema" }, description = "destination schema")
    public String schema = DEFAULT_SCHEMA;

    @Parameter(names = { "-f", "--format" }, description = "file format", converter = FormatConverter.class)
    public Format format = Format.AUTO;

    @Parameter(names = "--header", description = "CSV file has header")
    public boolean header;

    @Parameter(names = { "-t", "--into" }, description = "target table name")
    public String target;

    @Parameter(names = { "-n", "--threads" }, description = "number of threads")
    public int nthreads = 1;

    @Parameter(names = { "-c", "--commit" }, description = "commit every n rows", converter = CommitConverter.class)
    public Long commitFrequency;

    @Parameter(names = { "-r", "--retry" }, description = "number of times to try on transaction error")
    public int maxRetries = 10;

    @Parameter(names = { "-q", "--quiet" }, description = "no progress output")
    public boolean quiet;

    @Parameter(names = { "--constraint-check-time" }, description = "when to check uniqueness constraints", validateWith = ConstraintCheckTimeValidator.class)
    public String constraintCheckTime = "DEFERRED_WITH_RANGE_CACHE";

    @Parameter(description = "file(s)", required = true)
    public List<File> files = new ArrayList<>();
}
