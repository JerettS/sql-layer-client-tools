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

import com.beust.jcommander.JCommander;
import com.beust.jcommander.ParameterException;
import jline.Terminal;
import jline.TerminalFactory;
import jline.console.ConsoleReader;
import jline.console.UserInterruptException;
import jline.console.history.FileHistory;
import org.postgresql.util.PSQLState;

import java.io.BufferedInputStream;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileDescriptor;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;

import static com.foundationdb.sql.client.cli.BackslashQueries.*;

public class CLIClient
{
    private final static String APP_NAME = "fdbsqlcli";
    private final static File HISTORY_FILE = new File(System.getProperty("user.home"), "." + APP_NAME + "_history");


    public static void main(String[] args) throws Exception {
        CLIClientOptions options = new CLIClientOptions();
        try {
            JCommander jc = new JCommander(options, args);
            if(options.help) {
                jc.setProgramName(APP_NAME);
                jc.usage();
                return;
            }
        } catch(ParameterException e) {
            System.err.println(e.getMessage());
            System.exit(1);
        }
        // Positional arg overrides named parameter
        if(!options.positional.isEmpty()) {
            options.schema = options.positional.get(0);
        }
        if(options.positional.size() > 1) {
            System.err.print("extra command-line arguments ignored: ");
            System.err.println(options.positional.subList(1, options.positional.size()));
        }
        CLIClient client = new CLIClient(options);
        try {
            // --file takes preference over --command
            if(options.file != null) {
                checkOptionsFile(options.file);
                client.open_File(options.file);
            } else if(options.command != null) {
                client.open_String(options.command);
            } else {
                client.open_Standard();
            }
        } catch(Exception e) {
            System.err.println(e.getMessage());
            System.err.println("Connection details: " + client.getConnectionDescription());
            System.exit(1);
        }
        try {
            client.printTerminalInfo();
            client.printVersionInfo();
            client.runLoop();
        } finally {
            client.close();
        }
    }


    private CLIClientOptions options;
    private Terminal terminal;
    private ConsoleReader console;
    private Connection connection;
    private Statement statement;
    private Map<String,PreparedStatement> preparedStatements;
    private boolean withPrompt = true;
    private FileHistory fileHistory = null;
    private boolean isRunning = true;


    public CLIClient(CLIClientOptions options) {
        this.options = options;
    }

    public void close() throws Exception {
        if(fileHistory != null) {
            fileHistory.flush();
        }
        fileHistory = null;
        disconnect();
        console.flush();
        console.shutdown();
        terminal.restore();
        console = null;
    }

    public void runLoop() throws SQLException, IOException {
        ResultPrinter resultPrinter = new ResultPrinter(console.getOutput());
        QueryBuffer qb = new QueryBuffer();
        while(isRunning) {
            try {
                String prompt = withPrompt ? (qb.isEmpty() ? connection.getCatalog() + "=> " : "> ") : null;
                String str = console.readLine(prompt);
                if(str == null) {
                    // ctrl-d, exit
                    if(withPrompt) {
                        console.println();
                    }
                    break;
                }
                if(!qb.isEmpty()) {
                    qb.append(' '); // Collapsing multiple lines into one, add space
                    qb.append(str);
                } else if(hasNonSpace(str)) {
                    qb.append(str);
                }
            } catch(UserInterruptException e) {
                // ctrl-c, abort current query
                qb.reset();
            }
            while(isRunning && qb.hasQuery()) {
                boolean isBackslash = qb.isBackslash();
                String query = qb.nextQuery();
                try {
                    if(isBackslash) {
                        runBackslash(resultPrinter, query);
                    } else {
                        // TODO: No way to get the ResultSet *and* updateCount for RETURNING?
                        boolean res = statement.execute(query);
                        printWarnings(resultPrinter, statement);
                        if(res) {
                            ResultSet rs = statement.getResultSet();
                            resultPrinter.printResultSet(rs);
                            rs.close();
                        } else {
                            resultPrinter.printUpdate(statement.getUpdateCount());
                        }
                    }
                } catch(SQLException e) {
                    String state = e.getSQLState();
                    if(PSQLState.CONNECTION_FAILURE.getState().equals(state) ||
                       PSQLState.CONNECTION_FAILURE_DURING_TRANSACTION.getState().equals(state)) {
                        isRunning = tryReconnect(resultPrinter);
                    } else {
                        printWarnings(resultPrinter, statement);
                        resultPrinter.printError(e);
                    }
                }
                console.flush();
            }
            String completed = qb.trimCompleted();
            if(fileHistory != null) {
                fileHistory.add(completed);
            }
        }
    }


    //
    // Internal
    //

    void open_Standard() throws IOException, SQLException {
        // This is what the generic ConsoleReader() constructor does. Would System.in work?
        openInternal(new FileInputStream(FileDescriptor.in), System.out, true, true);
    }

    void open_String(String str) throws IOException, SQLException {
        str = str + "\n"; // Hack around ConsoleReader requirement
        openInternal(new ByteArrayInputStream(str.getBytes()), System.out, false, false);
    }

    void open_File(String fileIn) throws IOException, SQLException {
        openInternal(new BufferedInputStream(new FileInputStream(fileIn)), System.out, false, false);
    }

    void openInternal(InputStream in, OutputStream out, boolean withPrompt, boolean withHistory) throws IOException, SQLException {
        assert in != null;
        assert out != null;
        this.terminal = TerminalFactory.create();
        this.console = new ConsoleReader(APP_NAME, in, out, terminal);
        this.withPrompt = withPrompt;
        // Manually managed
        console.setHistoryEnabled(false);
        if(withHistory) {
            this.fileHistory = new FileHistory(HISTORY_FILE);
            console.setHistory(fileHistory);
        }
        // To catch ctrl-c
        console.setHandleUserInterrupt(true);
        connect();
    }

    private void connect() throws SQLException {
        String url = String.format("jdbc:fdbsql://%s:%d/%s", options.host, options.port, options.schema);
        connection = DriverManager.getConnection(url, options.user, options.password);
        statement = connection.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
        preparedStatements = new HashMap<>();
    }

    private void disconnect() throws SQLException {
        if(statement != null) {
            statement.close();
            statement = null;
        }
        if(preparedStatements != null) {
            for(PreparedStatement pStmt : preparedStatements.values()) {
                pStmt.close();
            }
            preparedStatements = null;
        }
        if(connection != null) {
            connection.close();
            connection = null;
        }
    }

    private void printTerminalInfo() throws SQLException, IOException {
        if(!terminal.isSupported()) {
            console.println("Warning: Unsupported terminal, line editing unavailable.");
        }
    }

    private void printVersionInfo() throws SQLException, IOException {
        DatabaseMetaData md = connection.getMetaData();
        console.println(String.format("fdbsql (driver %d.%d, layer %s)",
                                      md.getDriverMajorVersion(),
                                      md.getDriverMinorVersion(),
                                      md.getDatabaseProductVersion()));
    }

    private void printConnectionInfo() throws IOException {
        console.println(getConnectionDescription());
    }

    private void printBackslashHelp() throws IOException {
        int maxCmd = 0;
        int maxArg = 0;
        for(BackslashCommand cmd : BackslashCommand.values()) {
            maxCmd = Math.max(maxCmd, cmd.helpCmd.length());
            maxArg = Math.max(maxArg, cmd.helpArgs.length());
        }
        for(BackslashCommand cmd : BackslashCommand.values()) {
            console.println(String.format("  %-"+maxCmd+"s  %-"+maxArg+"s  %s", cmd.helpCmd, cmd.helpArgs, cmd.helpDesc));
        }
        console.println();
    }

    private void runBackslash(ResultPrinter printer, String input) throws SQLException, IOException {
        BackslashParser.Parsed parsed = BackslashParser.parseFrom(input);
        BackslashCommand command = lookupBackslashCommand(parsed);
        switch(command) {
            case CONNECT:
                disconnect();
                options.schema = parsed.argOr(0, options.schema);
                options.user = parsed.argOr(1, options.user);
                options.host = parsed.argOr(2, options.host);
                options.port = Integer.parseInt(parsed.argOr(3, Integer.toString(options.port)));
                connect();
                printVersionInfo();
                printConnectionInfo();
            break;
            case CONNINFO:
                printConnectionInfo();
            break;
            case HELP:
                printBackslashHelp();
            break;
            case QUIT:
                isRunning = false;
            break;
            default:
                runBackslash(printer, parsed, command);
        }
    }

    private void runBackslash(ResultPrinter printer, BackslashParser.Parsed parsed, BackslashCommand command) throws SQLException, IOException {
        String query = null;
        int expectedArgs = 0;
        String prepKey = parsed.getCanonical();
        boolean isSystem = parsed.isSystem;
        switch(command) {
            case L_ALL:
                query = listAll(parsed.isSystem);
                expectedArgs = 2;
            break;
            case L_INDEXES:
                query = listIndexes(parsed.isSystem, parsed.isDetail);
                expectedArgs = 3;
            break;
            case L_SEQUENCES:
                query = listSequences(parsed.isSystem, parsed.isDetail);
                expectedArgs = 2;
            break;
            case L_SCHEMAS:
                query = listSchemata(parsed.isSystem, parsed.isDetail);
                expectedArgs = 1;
            break;
            case D_TABLE:
                // If fully qualified, include system even without S
                isSystem = parsed.isSystem || (parsed.args.size() > 1);
                prepKey = BackslashParser.Parsed.getCanonical(BackslashCommand.L_TABLES.cmd, isSystem, parsed.isDetail);
            case L_TABLES:
                query = listTables(isSystem, parsed.isDetail);
                expectedArgs = 2;
            break;
            case D_VIEW:
                // If fully qualified, include system even without S
                isSystem = parsed.isSystem || (parsed.args.size() > 1);
                prepKey = BackslashParser.Parsed.getCanonical(BackslashCommand.L_VIEWS.cmd, isSystem, parsed.isDetail);
            case L_VIEWS:
                query = listViews(isSystem, parsed.isDetail);
                expectedArgs = 2;
            break;
        }

        if(query != null) {
            PreparedStatement pStmt = getPrepared(prepKey, query);
            String[] args = reverseFillParams(parsed, expectedArgs);
            for(int i = 0; i < args.length; ++i) {
                pStmt.setString(i + 1, args[i]);
            }
            ResultSet rs = pStmt.executeQuery();
            if(command == BackslashCommand.D_TABLE || command == BackslashCommand.D_VIEW) {
                String query2 = describeTableOrView(isSystem, parsed.isDetail);
                String typeDesc = (command == BackslashCommand.D_TABLE) ? "Table" : "View";
                PreparedStatement pStmt2 = getPrepared(parsed.getCanonical(), query2);
                while(rs.next()) {
                    String schema = rs.getString(1);
                    String table = rs.getString(2);
                    pStmt2.setString(1, schema);
                    pStmt2.setString(2, table);
                    ResultSet rs2 = pStmt2.executeQuery();
                    String description = String.format("%s %s.%s", typeDesc, schema, table);
                    printer.printResultSet(description, rs2);
                    rs2.close();
                }
            } else {
                printer.printResultSet(rs);
            }
            rs.close();
        }
    }

    private PreparedStatement getPrepared(String key, String statement) throws SQLException {
        PreparedStatement pStmt = preparedStatements.get(key);
        if(pStmt == null) {
            pStmt = connection.prepareStatement(statement, ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
            preparedStatements.put(key, pStmt);
        }
        return pStmt;
    }

    private String getConnectionDescription() {
        return String.format("%s@%s:%d/%s", options.user, options.host, options.port, options.schema);
    }

    private boolean tryReconnect(ResultPrinter printer) throws IOException {
        printer.printError("Lost connection to server... ");
        // Try to reconnect
        try {
            disconnect();
            connect();
            printer.printError("Reconnected");
            return true;
        } catch(SQLException e2) {
            printer.printError("Unable to reconnect");
            return false;
        }
    }

    //
    // Static
    //

    private static BackslashCommand lookupBackslashCommand(BackslashParser.Parsed parsed) throws SQLException {
        for(BackslashCommand c : BackslashCommand.values()) {
            if(c.cmd.equals(parsed.command) && (!parsed.isSystem || c.hasSystem) && (!parsed.isDetail || c.hasDetail)) {
                return c;
            }
        }
        throw new SQLException(String.format("Invalid command: \\%s. Try %s for help.", parsed.command, BackslashCommand.HELP.helpCmd));
    }

    private static String[] reverseFillParams(BackslashParser.Parsed parsed, int expected) {
        String[] out = new String[expected];
        int parsedIndex = parsed.args.size() - 1;
        for(int i = expected - 1; i >= 0; --i) {
            out[i] = parsed.argOr(parsedIndex--, "%");
        }
        return out;
    }

    private static boolean hasNonSpace(String s) {
        for(int i = 0; i < s.length(); ++i) {
            if(!Character.isWhitespace(s.charAt(i))) {
                return true;
            }
        }
        return false;
    }

    private static void printWarnings(ResultPrinter printer, Statement s) throws SQLException, IOException {
        SQLWarning warning = s.getWarnings();
        while(warning != null) {
            printer.printWarning(warning);
            warning = warning.getNextWarning();
        }
        s.clearWarnings();
    }

    private static void checkOptionsFile(String filename) {
        File file = new File(filename);
        String error = null;
        if(!file.exists()) {
            error = "no such file";
        }
        if(file.isDirectory()) {
            error = "is a directory";
        }
        if(error != null) {
            System.err.println(filename + ": " + error);
            System.exit(1);
        }
    }
}
