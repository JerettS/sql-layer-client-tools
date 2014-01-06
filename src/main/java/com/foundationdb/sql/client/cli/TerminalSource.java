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

import jline.Terminal;
import jline.TerminalFactory;
import jline.console.ConsoleReader;
import jline.console.UserInterruptException;
import jline.console.history.FileHistory;

import java.io.File;
import java.io.FileDescriptor;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Writer;

public class TerminalSource implements InputSource
{
    private Terminal terminal;
    private ConsoleReader console;
    private FileHistory fileHistory;

    public TerminalSource(String appName) throws IOException {
        // This is what the generic ConsoleReader() constructor does. Would System.in work?
        this(appName, new FileInputStream(FileDescriptor.in), System.out);
    }

    public TerminalSource(String appName, InputStream in, OutputStream out) throws IOException {
        this.terminal = TerminalFactory.create();
        this.console = new ConsoleReader(appName, in, out, terminal);
        // Manually managed
        console.setHistoryEnabled(false);
        // No '!' event expansion
        console.setExpandEvents(false);
        // To catch ctrl-c
        console.setHandleUserInterrupt(true);
    }

    //
    // InputSource
    //

    @Override
    public String getInfo() {
        return !terminal.isSupported() ? "Warning: Unsupported terminal, line editing unavailable." : null;
    }

    @Override
    public void setPrompt(String prompt) {
        console.setPrompt(prompt);
    }

    @Override
    public void openHistory(File file) throws IOException {
        fileHistory = new FileHistory(file);
        console.setHistory(fileHistory);
    }

    @Override
    public void addHistory(String line) {
        if(fileHistory != null) {
            fileHistory.add(line);
        }
    }

    @Override
    public String readLine() throws IOException {
        try {
            return console.readLine();
        } catch(UserInterruptException e) {
            throw new PartialLineException(e.getPartialLine());
        }
    }

    @Override
    public void close() {
        if(fileHistory != null) {
            try {
                fileHistory.flush();
            } catch(IOException e) {
                System.err.println(e.getMessage());
            }
        }
        if(console != null) {
            try {
                console.flush();
            } catch(IOException e) {
                System.err.println(e.getMessage());
            }
            console.shutdown();
        }
        if(terminal != null) {
            try {
                terminal.restore();
            } catch(Exception e) {
                System.err.println(e.getMessage());
            }
        }
        fileHistory = null;
        console = null;
        terminal = null;
    }

    //
    // TerminalSource
    //

    public Writer getConsoleWriter() {
        return console.getOutput();
    }
}
