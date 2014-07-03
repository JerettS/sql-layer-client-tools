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

import com.foundationdb.sql.client.cli.CLIClientOptions.OnErrorStatus;
import com.foundationdb.sql.client.cli.CLIClientOptions.OnErrorType;

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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class CLIClient implements Closeable
{
    private final static String PROGRAM_NAME = "fdbsqlcli";
    private final static File HISTORY_FILE = new File(System.getProperty("user.home"), "." + PROGRAM_NAME + "_history");

    private final static int MAX_PREPARED_RETRY = 5;
    private final static String STALE_STATEMENT_CODE = "0A50A";
    
    private static final String HELP_STRING = "help";
    private static final String EXIT_STRING = "exit";
    private static final String QUIT_STRING = "quit";
            

    private static final Map<BackslashCommand,BackslashQuery> LIST_QUERY;
    private static final Map<BackslashCommand,BackslashQuery> DESC_QUERY;

    static {
        LIST_QUERY = new HashMap<>();
        LIST_QUERY.put(BackslashCommand.L_ALL, BackslashQuery.LIST_ALL);
        LIST_QUERY.put(BackslashCommand.L_INDEXES, BackslashQuery.LIST_INDEXES);
        LIST_QUERY.put(BackslashCommand.L_SCHEMAS, BackslashQuery.LIST_SCHEMAS);
        LIST_QUERY.put(BackslashCommand.L_SEQUENCES, BackslashQuery.LIST_SEQUENCES);
        LIST_QUERY.put(BackslashCommand.L_TABLES, BackslashQuery.LIST_TABLES);
        LIST_QUERY.put(BackslashCommand.L_VIEWS, BackslashQuery.LIST_VIEWS);

        DESC_QUERY = new HashMap<>();
        DESC_QUERY.put(BackslashCommand.D_ALL, BackslashQuery.LIST_ALL);
        DESC_QUERY.put(BackslashCommand.D_SEQUENCE, BackslashQuery.LIST_SEQUENCES);
        DESC_QUERY.put(BackslashCommand.D_TABLE, BackslashQuery.LIST_TABLES);
        DESC_QUERY.put(BackslashCommand.D_VIEW, BackslashQuery.LIST_VIEWS);
    }


    /**
     * A private static entry point used for testing. 
     * @param args
     * @throws Exception
     */
    public static int test_main (String[] args) throws Exception  {
        return real_main (args);
    }
    
    /**
     * The real main() entry point for the fdbsqlcli program. Exits
     * with an return code > 0 if a statement failed. 
     * @param args - command line parameters
     * @throws Exception
     */
    public static void main(String[] args) throws Exception {
        System.exit(real_main(args));
    }
    
    /**
     * The real, full processing loop internals. 
     * @param args - command line parameters
     * @return exit code. 
     * @throws Exception
     */
    private static int real_main(String[] args) throws Exception {
        int lastError = 0;
        
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
            }
            // Connect to output file if output is requested. 
            if (options.output != null) {
                String input = "\\o " + options.output;
                client.execOutput(BackslashParser.parseFrom(input, false));
            }
            
            if (options.onError.size() == 2) {
                String input = "\\onerror " + options.onError.get(0) + " " + options.onError.get(1);
                client.toggleOnError(BackslashParser.parseFrom(input, false));
            }
            
        } catch(Exception e) {
            System.err.println(e.getMessage());
            if(e instanceof SQLException) {
                System.err.println("Connection details: " + client.getConnectionDescription());
            }
            return 1;
        }
        try {
            if(!options.quiet) {
                String inputInfo = client.source.getInfo();
                if(inputInfo != null) {
                    client.sink.println(inputInfo);
                }
                client.printVersionInfo();
            }
            lastError = client.runLoop();
        } finally {
            client.close();
        }
        return lastError;
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

    public int runLoop() throws Exception {
        return consumeSource(source, withPrompt, withQueryEcho);
    }

    private int consumeSource(InputSource localSource, boolean doPrompt, boolean doEcho) throws Exception {
        int lastError = 0;
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
                String str = localSource.readSome();
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
                    if (qb.isEmpty() && HELP_STRING.equalsIgnoreCase(str.trim())) {
                        qb.append("\\?\n");
                    } else if (qb.isEmpty() && 
                            (EXIT_STRING.equalsIgnoreCase(str.trim()) ||
                             QUIT_STRING.equalsIgnoreCase(str.trim()))) {
                        qb.append("\\q\n");
                    } else {
                        qb.append(str);
                    }
                }
            } catch(PartialLineException e) {
                // ctrl-c, abort current query
                qb.reset();
            }
            while(isConsuming && isRunning && qb.hasQuery()) {
                lastError = 0;
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
                        lastError = runBackslash(query);
                        if (options.onErrorType == OnErrorType.EXIT && lastError != 0) {
                            isRunning = false;
                        }
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
                        
                        switch (options.onErrorStatus) {
                        case CODE:
                            lastError = options.statusCode;
                            break;
                        case FAILURE:
                            lastError = 1;
                            break;
                        case SQLCODE:
                            if (state != null) {
                                lastError = Integer.parseInt(state.substring(0, 2), 36);
                                // Should never happen because the SQLCode values in the SQLLayer 
                                // as of the time of this merge request don't exceed 252. 
                                lastError = lastError > 255 ? 4 : lastError;
                            } else {
                                lastError = 3;
                            }
                            break;
                        case SUCCESS:
                            lastError = 0;
                            break;
                        }
                        if (options.onErrorType == OnErrorType.EXIT) {
                            isRunning = false;
                        }
                    }
                }
                sink.flush();
            }
            String completed = qb.trimCompleted().trim();
            if(!completed.isEmpty()) {
                String msg = localSource.addHistory(completed);
                if(msg != null) {
                    resultPrinter.printError(msg);
                    sink.flush();
                }
            }
        }
        return lastError;
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

    private int runBackslash(String input) throws Exception {
        int lastError = 0;
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
                lastError = execInput(BackslashParser.parseFrom(input, false));
            break;
            case O_FILE:
                // re-parse to not split on periods
                execOutput(BackslashParser.parseFrom(input, false));
            break;
            case TIMING:
                toggleShowTiming();
                printTimingStatus();
            break;
            case HELP:
                printBackslashHelp();
            break;
            case ON_ERROR:
                toggleOnError(parsed);
                break;
            case X_OUTPUT:
                if(!parsed.args.isEmpty()){
                    switch(parsed.args.get(0).toLowerCase()) {
                        case "on": {
                            resultPrinter.changeExpandedOutput(true);
                            sink.print("Expanded display is on\n");
                            break;
                        }
                        case "off": {
                            resultPrinter.changeExpandedOutput(false);
                            sink.print("Expanded display is off\n");
                            break;
                        }
                        default: {
                            sink.printlnError("Wrong argument type: expected [on|off]");
                        }
                    }

                } else{
                    resultPrinter.changeExpandedOutput();
                    String truth = (resultPrinter.getExpandedOutput())? "on": "off";
                    sink.print("Expanded display is " + truth + "\n");
                }
                break;
            case NULL:
                if(parsed.args.isEmpty()) {
                    resultPrinter.changeNullOutput();
                }else{
                    resultPrinter.changeNullOutput(parsed.args.get(0));
                }
                sink.print("Format NULL as \"" + resultPrinter.getNullString() + "\"\n");
                break;
            case TUPLE:
                if(!parsed.args.isEmpty()){
                    switch(parsed.args.get(0).toLowerCase()) {
                        case "on": {
                            resultPrinter.changeTupleOutput(true);
                            sink.print("Tuples only is on\n");
                            break;
                        }
                        case "off": {
                            resultPrinter.changeTupleOutput(false);
                            sink.print("Tuples only is off\n");
                            break;
                        }
                        default: {
                            sink.printlnError("Wrong argument type: expected [on|off]\n");
                            break;
                        }
                    }

                } else {
                    resultPrinter.changeTupleOutput();
                    String truth = (resultPrinter.getTupleOutput()) ? "on" : "off";
                    sink.print("Tuple only is " + truth + "\n");
                }
                break;
            case SEPARATOR:
                if(parsed.args.isEmpty()) {
                    resultPrinter.setFieldSeparator("|");
                }else{
                    resultPrinter.setFieldSeparator(parsed.args.get(0));
                }
                sink.print("Field separator is now \"" + resultPrinter.getFieldSeparator() + "\"\n");
                break;
            case ALIGNMENT:
                if(!parsed.args.isEmpty()){
                    switch(parsed.args.get(0).toLowerCase()) {
                        case "on": {
                            resultPrinter.changeAlignment(true);
                            sink.print("Output is aligned\n");
                            break;
                        }
                        case "off": {
                            resultPrinter.changeAlignment(false);
                            sink.print("Output is unaligned\n");
                            break;
                        }
                        default: {
                            sink.printlnError("Wrong argument type: expected [on|off]\n");
                            break;
                        }
                    }
                } else {
                    resultPrinter.changeAlignment();
                    String truth = (resultPrinter.getAlignment()) ? "aligned" : "unaligned";
                    sink.print("Output is  " + truth + "\n");
                }
                break;
            case QUIT:
                if (parsed.args.size() == 1) {
                    try {
                        Integer value = Integer.parseInt(parsed.args.get(0));
                        if (value >= 0 && value <= 255) {
                            lastError = value;
                        }
                    } catch (NumberFormatException e) {
                        // Explicitly do nothing.
                    }
                }
                isRunning = false;
                break;
            default:
                // If fully qualified, include system even without S
                parsed.isSystem = parsed.isSystem || (parsed.args.size() > 1);
                BackslashQuery query = LIST_QUERY.get(command);
                if(query != null) {
                    execList(query, parsed);
                }
                else if((query = DESC_QUERY.get(command)) != null) {
                    execDescribe(query, parsed);
                }
                else {
                    throw new SQLException("Unexpected command: " + command);
                }
            break;
        }
        return lastError;
    }

    private void toggleShowTiming() {
        showTiming = !showTiming;
    }
    
    
    private void toggleOnError(BackslashParser.Parsed parsed) throws IOException {
        if(parsed.args.isEmpty()) {
            // Do Nothing, the status is printed below on the way out
        } else if (parsed.args.size() == 1) {
            String typeName = parsed.argOr(0, CLIClientOptions.OnErrorType.CONTINUE.name());
            OnErrorType type = OnErrorType.fromString(typeName);
            if (type != OnErrorType.CONTINUE) {
                sink.printlnError ("Wrong error type: " + typeName + ", expected [CONTINUE|EXIT]");
                return;
            }
            options.onErrorType = type;
            options.onErrorStatus = OnErrorStatus.SUCCESS;
            
        } else if (parsed.args.size() == 2) {
            String typeName = parsed.argOr(0, CLIClientOptions.OnErrorType.CONTINUE.name());
            String valueName = parsed.argOr(1, CLIClientOptions.OnErrorStatus.SUCCESS.name());
        
            OnErrorType type = OnErrorType.fromString(typeName);
            if (type == null) {
                sink.printlnError ("Wrong error type: " + typeName + ", expected [CONTINUE|EXIT]");
                return;
            }
            OnErrorStatus status = OnErrorStatus.fromString(valueName);
            if (status == null) {
                sink.printlnError("Wrong onError status: " + valueName + ", expected [SUCCESS|FAILURE|SQLCODE|<n>]");
                return;
            }
            options.onErrorType= type;
            options.onErrorStatus = status;
            
            if (type == OnErrorType.CONTINUE) {
                options.onErrorStatus = OnErrorStatus.SUCCESS;
            }
            
            if (status == OnErrorStatus.CODE) {
                options.statusCode = Integer.parseInt(valueName);
            }
        } else {
            sink.printlnError("Missing on-error arguments, expected [CONTINUE|EXIT [SUCCESS|FAILURE|SQLCODE|<n>]]");
            return;
        }
        sink.println(String.format("On-Error is %s %s", options.onErrorType.name(), options.onErrorStatus.name()));
    }

    private void printTimingStatus() throws IOException{
        String status = "off";
        if (showTiming){
            status = "on";
        }
        sink.println(String.format("Timing is %s.", status));
    }

    private void execList(BackslashQuery query, BackslashParser.Parsed parsed) throws Exception {
        String[] args = reverseFillParams(parsed, query.argCount());
        try(ResultSet rs = execPrepared(query.build(parsed.isDetail, parsed.isSystem), args)) {
            resultPrinter.printResultSet(rs);
        }
    }

    private void execDescribe(BackslashQuery query, BackslashParser.Parsed parsed) throws Exception {
        String[] listArgs = reverseFillParams(parsed, 2);
        try(ResultSet lrs = execPrepared(query.build(parsed.isDetail, parsed.isSystem), listArgs)) {
            while(lrs.next()) {
                BackslashQuery descQuery = query.descQuery;
                if(descQuery == null) {
                    String type = lrs.getString(3);
                    if(type.contains("SEQUENCE")) {
                        descQuery = BackslashQuery.DESCRIBE_SEQUENCES;
                    } else if(type.contains("VIEW")) {
                        descQuery = BackslashQuery.DESCRIBE_VIEWS;
                    } else {
                        descQuery = BackslashQuery.DESCRIBE_TABLES;
                    }
                }
                String schemaName = lrs.getString(1);
                String tableName = lrs.getString(2);
                String queryStr = descQuery.build(parsed.isDetail, parsed.isSystem);
                try(ResultSet drs = execPrepared(queryStr, schemaName, tableName)) {
                    String description = String.format("%s %s.%s", descQuery.descType, schemaName, tableName);
                    resultPrinter.printResultSet(description, drs);
                }
                if(descQuery == BackslashQuery.DESCRIBE_TABLES) {
                    execDescribeTableExtra(schemaName, tableName);
                }
            }
        }
    }

    private void execDescribeTableExtra(String tableSchema, String tableName) throws Exception {
        execDescribeTableExtra(BackslashQuery.EXTRA_TABLE_INDEXES, tableSchema, tableName,
                               true, "Indexes", "%s%s (%s)%s", -1);

        boolean first = execDescribeTableExtra(BackslashQuery.EXTRA_TABLE_FK_REFERENCES, tableSchema, tableName,
                                               true, "References", "%s FOREIGN KEY (%s) REFERENCES %s (%s)", -1);
        execDescribeTableExtra(BackslashQuery.EXTRA_TABLE_GFK_REFERENCES, tableSchema, tableName,
                               first, "References", "GROUPING FOREIGN KEY (%s) REFERENCES %s (%s)", 1);

        first = execDescribeTableExtra(BackslashQuery.EXTRA_TABLE_FK_REFERENCED_BY, tableSchema, tableName,
                                       true, "Referenced By", "TABLE %s CONSTRAINT %s FOREIGN KEY (%s) REFERENCES %s (%s)", -1);
        execDescribeTableExtra(BackslashQuery.EXTRA_TABLE_GFK_REFERENCED_BY, tableSchema, tableName,
                               first, "Referenced By", "TABLE %s GROUPING FOREIGN KEY (%s) REFERENCES %s (%s)", 2);
        sink.println();
    }

    private boolean execDescribeTableExtra(BackslashQuery query, String schema, String table,
                                           boolean first, String description, String format, int skipArg) throws Exception {
        List<String> args = new ArrayList<>();
        try(ResultSet rs = execPrepared(query.build(false, false), schema, table)) {
            while(rs.next()) {
                if(first) {
                    sink.print(description);
                    sink.println(":");
                    first = false;
                }
                for(int i = 1; i <= rs.getMetaData().getColumnCount(); ++i) {
                    if(i != skipArg) {
                        args.add(rs.getString(i));
                    }
                }
                sink.print("    ");
                sink.println(String.format(format, args.toArray(new String[args.size()])));
                args.clear();
            }
        }
        return first;
    }

    private int execInput(BackslashParser.Parsed parsed) throws Exception {
        int lastError = 0;
        if(parsed.args.isEmpty()) {
            sink.printlnError("Missing file argument");
        } else {
            try {
                FileReader reader = new FileReader(new File(options.includedParent, parsed.args.get(0)));
                InputSource localSource = new ReaderSource(reader);
                lastError = consumeSource(localSource, false, true);
            } catch(FileNotFoundException e) {
                sink.printlnError(e.getMessage());
            }
        }
        return lastError;
    }

    private void execOutput(BackslashParser.Parsed parsed) throws Exception {
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

    private ResultSet execPrepared(String query, String... args) throws SQLException {
        for(int retry = 0; retry < MAX_PREPARED_RETRY; ++retry) {
            try {
                PreparedStatement pStmt = preparedStatements.get(query);
                if(pStmt == null) {
                    pStmt = connection.prepareStatement(query, ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
                    preparedStatements.put(query, pStmt);
                }
                for(int i = 0; i < args.length; ++i) {
                    pStmt.setString(i + 1, args[i]);
                }
                return pStmt.executeQuery();
            } catch(SQLException e) {
                if(!STALE_STATEMENT_CODE.equals(e.getSQLState())) {
                    throw e;
                }
                PreparedStatement pStmt = preparedStatements.remove(query);
                if(pStmt != null) {
                    pStmt.close();
                }
            }
        }
        throw new IllegalStateException("Unable to exec prepared statement");
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
