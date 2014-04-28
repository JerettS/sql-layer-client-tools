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

// jansi-native is bundled with jline
import org.fusesource.jansi.internal.CLibrary;
import org.postgresql.util.PSQLState;

import java.io.Closeable;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
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

public class CLIClient implements Closeable
{
    private final static String PROGRAM_NAME = "fdbsqlcli";
    private final static File HISTORY_FILE = new File(System.getProperty("user.home"), "." + PROGRAM_NAME + "_history");

    private final static int MAX_PREPARED_RETRY = 5;
    private final static String STALE_STATEMENT_CODE = "0A50A";


    public static void main(String[] args) throws Exception {
        CLIClientOptions options = new CLIClientOptions();
        options.parseOrDie(PROGRAM_NAME, args);
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
            File configFile = new File(options.configFileName);
            if (configFile.isFile() && !options.skipRC){
                client.openFile(options.configFileName);
                client.runLoop();
            }
            // Auto-quiet if non-interactive input source.
            // --file takes preference over --command
            if(options.file != null) {
                options.quiet = true;
                client.openFile(options.file);
            } else if(options.command != null) {
                options.quiet = true;
                client.openString(options.command);
            } else {
                // Disable fancy terminal if input is not interactive.
                if(CLibrary.HAVE_ISATTY && (CLibrary.isatty(CLibrary.STDIN_FILENO) == 0)) {
                    options.quiet = true;
                    client.openSimpleTerminal();
                } else {
                    client.openTerminal();
                }
                // Connect to output file if output is requested. 
                if (options.output != null) {
                    client.runBackslashO(BackslashParser.parseFrom(options.output, false));
                }
            }
        } catch(Exception e) {
            System.err.println(e.getMessage());
            if(e instanceof SQLException) {
                System.err.println("Connection details: " + client.getConnectionDescription());
            }
            System.exit(1);
        }
        try {
            if(!options.quiet) {
                String inputInfo = client.source.getInfo();
                if(inputInfo != null) {
                    client.sink.println(inputInfo);
                }
                client.printVersionInfo();
            }
            client.runLoop();
        } finally {
            client.close();
        }
    }


    private CLIClientOptions options;
    private InputSource source;
    private OutputSink sink;
    private OutputSink otherSink;
    private ResultPrinter resultPrinter;
    private boolean withPrompt = true;
    private boolean isRunning = true;
    private boolean withQueryEcho = false;
    private boolean showTiming = false;

    private Connection connection;
    private Statement statement;
    private Map<String,PreparedStatement> preparedStatements;


    public CLIClient(CLIClientOptions options) {
        this.options = options;
    }

    public void close() {
        if(source != null) {
            source.close();
            source = null;
        }
        if(otherSink != null) {
            try {
                otherSink.close();
            } catch(IOException e) {
                // Ignore
            }
            otherSink = null;
        }
        try {
            disconnect();
        } catch(SQLException e) {
            // Ignore
        }
    }

    public void runLoop() throws Exception {
        consumeSource(source, withPrompt, withQueryEcho);
    }

    private void consumeSource(InputSource localSource, boolean doPrompt, boolean doEcho) throws Exception {
        QueryBuffer qb = new QueryBuffer();
        boolean isConsuming = true;
        while(isConsuming && isRunning) {
            if(!qb.hasNonSpace()) {
                qb.reset();
            }
            try {
                if(doPrompt) {
                    String prompt = qb.isEmpty() ? (connection.getCatalog() + "=> ") : (qb.quoteString() + "> ");
                    localSource.setPrompt(prompt);
                }
                String str = localSource.readLine();
                // ctrl-d if interactive, exhausted source otherwise
                if(str == null) {
                    if(doPrompt) {
                        sink.println();
                    }
                    // May be buffer contents if non-interactive and no trailing semi or newline
                    if(!qb.isEmpty()) {
                        qb.setConsumeRemaining();
                    } else {
                        isConsuming = false;
                    }
                } else {
                    if(!qb.isEmpty()) {
                        qb.append(' '); // Collapsing multiple lines into one, add space
                        qb.appendLine(str);
                    } else {
                        qb.appendLine(str);
                    }
                }
            } catch(PartialLineException e) {
                // ctrl-c, abort current query
                qb.reset();
            }
            while(isConsuming && isRunning && qb.hasQuery()) {
                boolean isBackslash = qb.isBackslash();
                String query = qb.nextQuery();
                // User friendly: don't send empty or only semi, which will give a parse error
                if(hasOnlySpaceAndSemi(query)) {
                    continue;
                }
                try {
                    if(doEcho) {
                        sink.println(query);
                    }
                    if(isBackslash) {
                        runBackslash(resultPrinter, query);

                    } else {
                        // TODO: No way to get the ResultSet *and* updateCount for RETURNING?

                        long startTime = System.currentTimeMillis();
                        boolean res = statement.execute(query);
                        long endTime = System.currentTimeMillis();
                        printWarnings(statement);
                        if(res) {
                            ResultSet rs = statement.getResultSet();
                            resultPrinter.printResultSet(rs);
                            rs.close();
                        
                        } else {
                            resultPrinter.printUpdate(statement.getUpdateCount());
                        }
                        if (showTiming) {
                        Long totalTime = (endTime-startTime);
                        sink.println("Time: " + totalTime.toString()+ " ms");
                        }
                    }
                
                } catch(SQLException e) {
                    String state = e.getSQLState();
                    if(PSQLState.CONNECTION_FAILURE.getState().equals(state) ||
                       PSQLState.CONNECTION_FAILURE_DURING_TRANSACTION.getState().equals(state)) {
                        isRunning = tryReconnect();
                    } else {
                        printWarnings(statement);
                        resultPrinter.printError(e);
                    }
                }
                sink.flush();
            }
            String completed = qb.trimCompleted();
            if(!completed.isEmpty()) {
                localSource.addHistory(completed);
            }
        }
    }


    //
    // Internal
    //

    void openSimpleTerminal() throws IOException, SQLException {
        openInternal(new ReaderSource(new InputStreamReader(System.in)), createStandardSink(), false, false, true);
    }

    void openTerminal() throws IOException, SQLException {
        // This is what the generic ConsoleReader() constructor does. Would System.in work?
        TerminalSource terminalSource = new TerminalSource(PROGRAM_NAME);
        WriterSink writerSink = new WriterSink(terminalSource.getConsoleWriter(), new PrintWriter(System.err));
        openInternal(terminalSource, writerSink, true, true, false);
    }

    void openString(String str) throws IOException, SQLException {
        openInternal(new StringSource(str), createStandardSink(), false, false, false);
    }

    void openFile(String fileIn) throws IOException, SQLException {
        openInternal(new ReaderSource(new FileReader(fileIn)), createStandardSink(), false, false, true);
    }

    void openInternal(InputSource source, OutputSink sink, boolean withPrompt, boolean withHistory, boolean withQueryEcho) throws IOException, SQLException {
        assert source != null;
        assert sink != null;
        this.withPrompt = withPrompt;
        this.withQueryEcho = withQueryEcho;
        // is not null if rc file is present
        if (this.source != null){
            this.source.close();
        }
        this.source = source;
        this.otherSink = null;
        this.sink = sink;
        if(withHistory) {
            source.openHistory(HISTORY_FILE);
        }
        // will already be open if there was an rc file
        if (connection == null) {
            connect();
        }
        this.resultPrinter = new ResultPrinter(sink);
    }

    private void connect() throws SQLException {
        String url = options.getURL(options.schema) + options.urlOptions;
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

    private void printVersionInfo() throws SQLException, IOException {
        DatabaseMetaData md = connection.getMetaData();
        sink.println(String.format("fdbsql (driver %d.%d, layer %s)",
                                   md.getDriverMajorVersion(),
                                   md.getDriverMinorVersion(),
                                   md.getDatabaseProductVersion()));
    }

    private void printConnectionInfo() throws IOException {
        sink.println(getConnectionDescription());
    }

    private void printBackslashHelp() throws IOException {
        int maxCmd = 0;
        int maxArg = 0;
        for(BackslashCommand cmd : BackslashCommand.values()) {
            maxCmd = Math.max(maxCmd, cmd.helpCmd.length());
            maxArg = Math.max(maxArg, cmd.helpArgs.length());
        }
        sink.println();
        sink.println("Built-in commands are described below:");
        sink.println();
        sink.println(String.format("  %-"+maxCmd+"s  %-"+maxArg+"s  %s", "Command", "Options", "Description"));
        for(BackslashCommand cmd : BackslashCommand.values()) {
            sink.println(String.format("  %-"+maxCmd+"s  %-"+maxArg+"s  %s", cmd.helpCmd, cmd.helpArgs, cmd.helpDesc));
        }
        sink.println();
        sink.println("[+] Shows additional information on items");
        sink.println("[S] Lists all user and system tables");
        sink.println();
        sink.println("Usage example: \\ltS              list all tables including system information");
        sink.println("               \\lt+              list all user tables and include additional detail");
        sink.println("               \\i file_name.sql  process all commands from file");
        sink.println();              
    }

    private void runBackslash(ResultPrinter printer, String input) throws Exception {
        input = input.trim();
        if (input.endsWith(";")){
            input = input.substring(0, input.length()-1);
        }
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
            case I_FILE:
                // re-parse to not split on periods
                runBackslashI(BackslashParser.parseFrom(input, false));
            break;
            case O_FILE:
                // re-parse to not split on periods
                runBackslashO(BackslashParser.parseFrom(input, false));
            break;
            case TIMING:
                toggleShowTiming();
                printTimingStatus();
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

    private void toggleShowTiming() {
        showTiming = !showTiming;
    }
    
    private void printTimingStatus() throws IOException{
        String status = "off";
        if (showTiming){
            status = "on";
        }
        sink.println(String.format("Timing is %s.", status));
    }

    private void runBackslash(ResultPrinter printer, BackslashParser.Parsed parsed, BackslashCommand command) throws Exception {
        final String query;
        final int expectedArgs;
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
            default:
                throw new SQLException("Unexpected command: " + command);
        }

        String[] args = reverseFillParams(parsed, expectedArgs);
        ResultSet rs = execPrepared(prepKey, query, args);
        if(command == BackslashCommand.D_TABLE || command == BackslashCommand.D_VIEW) {
            String query2 = describeTableOrView(isSystem, parsed.isDetail);
            String typeDesc = (command == BackslashCommand.D_TABLE) ? "Table" : "View";
            while(rs.next()) {
                String[] args2 = { rs.getString(1), rs.getString(2) };
                ResultSet rs2 = execPrepared(parsed.getCanonical(), query2, args2);
                String description = String.format("%s %s.%s", typeDesc, args2[0], args2[1]);
                printer.printResultSet(description, rs2);
                rs2.close();
            }
        } else {
            printer.printResultSet(rs);
        }
        rs.close();
    }

    private void runBackslashI(BackslashParser.Parsed parsed) throws Exception {
        if(parsed.args.isEmpty()) {
            sink.printlnError("Missing file argument");
        } else {
            try {
                FileReader reader = new FileReader(new File(options.includedParent, parsed.args.get(0)));
                InputSource localSource = new ReaderSource(reader);
                consumeSource(localSource, false, true);
            } catch(FileNotFoundException e) {
                sink.printlnError(e.getMessage());
            }
        }
    }

    private void runBackslashO(BackslashParser.Parsed parsed) throws Exception {
        if(otherSink != null) {
            otherSink.close();
            otherSink = null;
            resultPrinter.setSink(sink);
        }
        if(!parsed.args.isEmpty() && !"-".equals(parsed.args.get(0).trim())) {
            try {
                File file = new File(options.includedParent, parsed.args.get(0));
                FileWriter fileOut = new FileWriter(file);
                otherSink = new WriterSink(fileOut, new PrintWriter(System.err), true, false);
                resultPrinter.setSink(otherSink);
            } catch(FileNotFoundException e) {
                sink.printlnError(e.getMessage());
            }
        }
    }

    private ResultSet execPrepared(String prepKey, String query, String[] args) throws SQLException {
        for(int retry = 0; retry < MAX_PREPARED_RETRY; ++retry) {
            try {
                PreparedStatement pStmt = getPrepared(prepKey, query);
                for(int i = 0; i < args.length; ++i) {
                    pStmt.setString(i + 1, args[i]);
                }
                return pStmt.executeQuery();
            } catch(SQLException e) {
                if(!STALE_STATEMENT_CODE.equals(e.getSQLState())) {
                    throw e;
                }
                closePrepared(prepKey);
            }
        }
        throw new IllegalStateException("Unable to exec prepared statement");
    }

    private void closePrepared(String key) throws SQLException {
        PreparedStatement pStmt = preparedStatements.remove(key);
        if(pStmt != null) {
            pStmt.close();
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

    private boolean tryReconnect() throws IOException {
        resultPrinter.printError("Lost connection to server... ");
        // Try to reconnect
        try {
            disconnect();
            connect();
            resultPrinter.printError("Reconnected");
            return true;
        } catch(SQLException e2) {
            resultPrinter.printError("Unable to reconnect");
            return false;
        }
    }

    private void printWarnings(Statement s) throws SQLException, IOException {
        SQLWarning warning = s.getWarnings();
        while(warning != null) {
            resultPrinter.printWarning(warning);
            warning = warning.getNextWarning();
        }
        s.clearWarnings();
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

    private static boolean hasOnlySpaceAndSemi(String s) {
        for(int i = 0; i < s.length(); ++i) {
            char c = s.charAt(i);
            if(!Character.isWhitespace(c) && c != ';') {
                return false;
            }
        }
        return true;
    }

    private static OutputSink createStandardSink() {
        return new PrintStreamSink(System.out, System.err);
    }
}
