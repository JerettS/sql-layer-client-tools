/**
 * Copyright (C) 2012 Akiban Technologies Inc.
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

package com.foundationdb.sql.client.dump;

import java.io.*;
import java.sql.*;
import java.util.*;

import org.postgresql.copy.CopyManager;

public class DumpClient
{
    protected static final String DRIVER_NAME = "org.postgresql.Driver";
    protected static final String DEFAULT_HOST = "localhost";
    protected static final int DEFAULT_PORT = 15432;
    protected static final int DEFAULT_INSERT_MAX_ROW_COUNT = 100;
    private static final String NL = System.getProperty("line.separator");
    private boolean dumpSchema = true, dumpData = true;
    private File outputFile = null;
    private String host = DEFAULT_HOST;
    private int port = DEFAULT_PORT;
    private String user = "system";
    private String password = "system";
    private Map<String,Map<String,Table>> schemas = new TreeMap<String,Map<String,Table>>();
    private Map<String, Map<String,Sequence>> sequences = new TreeMap<String,Map<String, Sequence>>();
    private Map<String,Map<String,View>> views = new TreeMap<String,Map<String, View>>();
    private Queue<String> afterDataStatements = new ArrayDeque<String>();
    private int insertMaxRowCount = DEFAULT_INSERT_MAX_ROW_COUNT;
    private String defaultSchema = null;
    private Writer output;
    private Connection connection;
    private CopyManager copyManager;

    public static void main(String[] args) throws Exception {
        DumpClient dumpClient = new DumpClient();
        dumpClient.parseArgs(args);
        try {
            dumpClient.dump();
        }
        finally {
            dumpClient.close();
        }
    }

    protected void usage() {
        System.out.println("DumpClient [--no-schemas] [-no-data] [-o file] [-h host] [-p port] [-u user] [-w password] schemas...");
        System.out.println("If no schemas are given, all are dumped.");
    }

    protected void parseArgs(String[] args) throws Exception {
        int i = 0;
        while (i < args.length) {
            String arg = args[i++];
            if (arg.startsWith("-")) {
                if ("--help".equals(arg)) {
                    usage();
                    System.exit(0);
                }
                else if ("-s".equals(arg) || ("--no-schemas".equals(arg))) {
                    setDumpSchema(false);
                }
                else if ("-d".equals(arg) || ("--no-data".equals(arg))) {
                    setDumpData(false);
                }
                else if ("-o".equals(arg) || ("--output".equals(arg))) {
                    setOutputFile(new File(args[i++]));
                }
                else if ("-h".equals(arg) || ("--host".equals(arg))) {
                    setHost(args[i++]);
                }
                else if ("-p".equals(arg) || ("--port".equals(arg))) {
                    setPort(Integer.parseInt(args[i++]));
                }
                else if ("-u".equals(arg) || ("--user".equals(arg))) {
                    setUser(args[i++]);
                }
                else if ("-w".equals(arg) || ("--password".equals(arg))) {
                    setPassword(args[i++]);
                }
                else if ("--insert-max-rows".equals(arg)) {
                    setInsertMaxRowCount(Integer.parseInt(args[i++]));
                }
                else {
                    throw new Exception("Unknown switch: " + arg);
                }
            }
            else {
                addSchema(arg);
            }
        }
    }

    public DumpClient() {
    }

    public boolean isDumpSchema() {
        return dumpSchema;
    }
    public void setDumpSchema(boolean dumpSchema) {
        this.dumpSchema = dumpSchema;
    }
    public boolean isDumpData() {
        return dumpData;
    }
    public void setDumpData(boolean dumpData) {
        this.dumpData = dumpData;
    }
    public File getOutputFile() {
        return outputFile;
    }
    public void setOutputFile(File outputFile) {
        this.outputFile = outputFile;
    }
    public String getHost() {
        return host;
    }
    public void setHost(String host) {
        this.host = host;
    }
    public int getPort() {
        return port;
    }
    public void setPort(int port) {
        this.port = port;
    }
    public String getUser() {
        return user;
    }
    public void setUser(String user) {
        this.user = user;
    }
    public String getPassword() {
        return password;
    }
    public void setPassword(String password) {
        this.password = password;
    }
    public int getInsertMaxRowCount() {
        return insertMaxRowCount;
    }
    public void setInsertMaxRowCount(int insertMaxRowCount) {
        this.insertMaxRowCount = insertMaxRowCount;
    }

    public Collection<String> getSchemas() {
        return schemas.keySet();
    }
    public void addSchema(String schema) {
        schemas.put(schema, new TreeMap<String,Table>());
        sequences.put(schema, new TreeMap<String,Sequence>());
        views.put(schema, new TreeMap<String,View>());
    }

    public void dump() throws Exception {
        openOutput();
        if (schemas.size() == 1) {
            defaultSchema = schemas.keySet().iterator().next();
        }
        openConnection();
        if (schemas.isEmpty()) {
            Statement stmt = connection.createStatement();
            ResultSet rs = stmt.executeQuery("SELECT schema_name FROM information_schema.schemata");
            while (rs.next()) {
                String name = rs.getString(1);
                if (!"information_schema".equals(name) &&
                    !"security_schema".equals(name)) {
                    addSchema(name);
                }
            }
            stmt.close();
        }
        if (schemas.isEmpty()) {
            System.err.println("Database is empty.");
        }
        else {
            for (String schema : schemas.keySet()) {
                loadTables(schema);
                loadSequences(schema);
            }
            // Keep doing this as long as there are references to new schemas.
            Deque<String> pending = new ArrayDeque<String>(schemas.keySet());
            while (!pending.isEmpty()) {
                loadGroups(pending.removeFirst(), pending);
            }
            // Have all the tables we will dump.
            loadViews();
            for (String schema : schemas.keySet()) {
                if (dumpSchema) {
                    dumpSequences(schema);
                    dumpViews(schema);
                }
                for (Table table : schemas.get(schema).values()) {
                    if (table.parent == null) {
                        dumpGroup(table);
                    }
                }
            }
        }
        close();
    }
    
    protected void loadTables(String schema) throws SQLException {
        Map<String,Table> tables = schemas.get(schema);
        PreparedStatement stmt = connection.prepareStatement("SELECT table_name FROM information_schema.tables WHERE table_schema = ? AND table_type = 'TABLE' ORDER BY table_id");
        stmt.setString(1, schema);
        ResultSet rs = stmt.executeQuery();
        while (rs.next()) {
            String name = rs.getString(1);
            tables.put(name, new Table(schema, name));
        }
        stmt.close();
    }
    
    protected void loadSequences (String schema) throws SQLException {
        Map<String, Sequence> seqs = sequences.get(schema);
        PreparedStatement stmt = connection.prepareStatement(
                "select sequence_schema, sequence_name, " + 
                " nextval(sequence_schema, sequence_name), " +
                " increment, minimum_value, maximum_value, cycle_option " +
                " from information_schema.sequences where sequence_schema = ? order by sequence_name");
        stmt.setString(1, schema);
        ResultSet rs = stmt.executeQuery();
        while (rs.next()) {
            boolean cycle = rs.getString(7).equals("YES");
            String name = rs.getString(2);
            seqs.put(name, new Sequence (rs.getString(1), name, 
                    rs.getLong(3), rs.getLong(4),rs.getLong(5), rs.getLong(6),
                    cycle));
        }
    }

    protected void loadGroups(String schema, Deque<String> pending) throws SQLException {
        PreparedStatement kstmt = connection.prepareStatement("SELECT column_name FROM information_schema.key_column_usage WHERE schema_name = ? and table_name = ? AND constraint_name = ? ORDER BY ordinal_position");
        PreparedStatement stmt = connection.prepareStatement("SELECT constraint_schema_name, constraint_table_name, unique_schema_name, unique_table_name, constraint_name, unique_constraint_name FROM information_schema.grouping_constraints WHERE constraint_schema_name = ? OR unique_schema_name = ?");
        stmt.setString(1, schema);
        stmt.setString(2, schema);
        ResultSet rs = stmt.executeQuery();
        while (rs.next()) {
            // TODO: IS NOT NULL would have been simpler, but schema is wrong.
            // Moreover, grouping_constraints table could have been
            // used more intelligently to get whole groups.
            if (rs.getString(4) == null) continue;
            Table child = findOrCreateTable(rs.getString(1), rs.getString(2), pending);
            Table parent = findOrCreateTable(rs.getString(3), rs.getString(4), pending);
            child.parent = parent;
            parent.children.add(child);
            String constraint = rs.getString(5);
            child.childKeys = loadKeys(kstmt, child.schema, child.name, constraint);
            constraint = rs.getString(6);
            List<String> keys = null;
            if ("PRIMARY".equals(constraint))
                keys = parent.primaryKeys;
            if (keys == null)
                keys = loadKeys(kstmt, parent.schema, parent.name, constraint);
            child.parentKeys = keys;
            if ((parent.primaryKeys == null) && "PRIMARY".equals(constraint))
                parent.primaryKeys = keys;
        }
        stmt.close();
        for (Table table : schemas.get(schema).values()) {
            if (table.primaryKeys == null) {
                table.primaryKeys = loadKeys(kstmt, table.schema, table.name, "PRIMARY");
            }
        }
        kstmt.close();
    }
    
    protected List<String> loadKeys(PreparedStatement kstmt, String schema, String table, String constraint) throws SQLException {
        List<String> keys = new ArrayList<String>();
        kstmt.setString(1, schema);
        kstmt.setString(2, table);
        kstmt.setString(3, constraint);
        ResultSet rs = kstmt.executeQuery();
        while (rs.next()) {
            keys.add(rs.getString(1));
        }
        rs.close();
        return keys;
    }

    protected void loadViews() throws SQLException {
        PreparedStatement stmt = connection.prepareStatement("SELECT table_name, view_definition FROM information_schema.views WHERE schema_name = ? ORDER BY table_name");
        for (Map.Entry<String,Map<String,View>> entry : views.entrySet()) {
            String schema = entry.getKey();
            Map<String,View> views = entry.getValue();
            stmt.setString(1, schema);
            ResultSet rs = stmt.executeQuery();
            while (rs.next()) {
                String name = rs.getString(1);
                views.put(name, new View(schema, name, rs.getString(2)));
            }
            rs.close();
        }
        stmt.close();
        stmt = connection.prepareStatement("SELECT table_schema, table_name FROM information_schema.view_table_usage WHERE view_schema = ? AND view_name = ?");
        for (Map.Entry<String,Map<String,View>> entry : views.entrySet()) {
            stmt.setString(1, entry.getKey());
            for (View view : entry.getValue().values()) {
                stmt.setString(2, view.name);
                ResultSet rs = stmt.executeQuery();
                while (rs.next()) {
                    String schema = rs.getString(1);
                    String name = rs.getString(2);
                    Viewed viewed = null;
                    Map<String,Table> stables = schemas.get(schema);
                    if (stables != null)
                        viewed = stables.get(name);
                    if (viewed == null) {
                        Map<String,View> sviews = views.get(schema);
                        if (sviews != null)
                            viewed = sviews.get(name);
                    }
                    if (viewed != null) {
                        view.dependsOn.add(viewed);
                        viewed.dependedOn.add(view);
                    }
                }
                rs.close();
            }
        }
        stmt.close();
    }

    protected static class Named implements Comparable<Named> {
        String schema, name;
        
        public Named(String schema, String name) {
            this.schema = schema;
            this.name = name;
        }

        @Override
        public int compareTo(Named o) {
            int cmp = schema.compareTo(o.schema);
            return cmp == 0 ? name.compareTo(o.name) : cmp;
        }
    }

    protected static class Viewed extends Named {
        Set<View> dependedOn = new TreeSet<View>();
        boolean dropped, dumped;
        
        public Viewed(String schema, String name) {
            super(schema, name);
        }
    }

    protected static class Table extends Viewed {
        Table parent;
        List<Table> children = new ArrayList<Table>();
        List<String> primaryKeys, childKeys, parentKeys;
        
        public Table(String schema, String name) {
            super(schema, name);
        }
    }
    
    protected static class View extends Viewed {
        String definition;
        Set<Viewed> dependsOn = new TreeSet<Viewed>();
        
        public View(String schema, String name, String definition) {
            super(schema, name);
            this.definition = definition;
        }
    }

    protected static class Sequence extends Named {
        long startWith, incrementBy;
        long minValue, maxValue;
        boolean cycle;
        
        public Sequence (String schema, String name, 
                long startWith, long incrementBy,
                long minValue, long maxValue,
                boolean cycle) {
            super(schema, name);
            this.startWith = startWith;
            this.incrementBy = incrementBy;
            this.minValue = minValue;
            this.maxValue = maxValue;
            this.cycle = cycle;
        }
    }

    protected Table findOrCreateTable(String schema, String name, Deque<String> pending) {
        Map<String,Table> tables = schemas.get(schema);
        if (tables == null) {
            // This is the odd case of a reference to some table not in a schema that
            // was requested. Have to get that schema anyway.
            tables = new TreeMap<String,Table>();
            schemas.put(schema, tables);
            pending.addLast(schema);
        }
        Table table = tables.get(name);
        if (table == null) {
            table = new Table(schema, name);
            tables.put(name, table);
        }
        return table;
    }
    
    protected void dumpSequences(String schema) throws IOException {
        StringBuilder sql = new StringBuilder();
        
        // drop sequences
        for (Sequence seq : sequences.get(schema).values()) {
            // these are sequences used for column identity generator
            if (seq.name.startsWith("_sequence")) {
                continue;
            }
            sql.append ("DROP SEQUENCE IF EXISTS ");
            qualifiedName(seq, sql);
            sql.append(" RESTRICT");
            sql.append(";").append(NL);
        }
        sql.append(NL);
        // create sequences
        for (Sequence seq : sequences.get(schema).values()) {
            // these are sequences used for column identity generator
            if (seq.name.startsWith("_sequence-")) {
                continue;
            }
            sql.append("CREATE SEQUENCE ");
            qualifiedName(seq, sql);
            sql.append(" START WITH ").append(seq.startWith);
            sql.append(" INCREMENT BY ").append(seq.incrementBy);
            sql.append(" MINVALUE ").append(seq.minValue);
            sql.append(" MAXVALUE ").append(seq.maxValue);          
            if (!seq.cycle) { sql.append(" NO"); }
            sql.append(" CYCLE"); 
            sql.append(";").append(NL);
        }
        output.write(sql.toString());
    }
    
    protected void dumpGroup(Table table) throws SQLException, IOException {
        outputGroupSummary(table, 1);
        output.write(NL);
        if (dumpSchema) {
            Set<View> views = new TreeSet<View>();
            dependentViews(table, views);
            if (!views.isEmpty())
                ensureDropViews(views);
            if (!table.children.isEmpty())
                outputDropGroup(table);
            outputDropTables(table);
            output.write(NL);
            outputCreateTables(table);
            output.write(NL);
            outputCreateIndexes(table);
            output.write(NL);
            if (!views.isEmpty())
                dumpViews(views);
        }
        if (dumpData) {
            dumpData(table);
            if (!afterDataStatements.isEmpty()) {
                String stmt;
                while ((stmt = afterDataStatements.poll()) != null) {
                    output.write(stmt);
                }
                output.write(NL);
            }
        }
    }

    protected void outputGroupSummary(Table table, int depth) throws IOException {
        StringBuilder summary = new StringBuilder("---");
        for (int i = 0; i < depth; i++)
            summary.append(' ');
        if (!table.schema.equals(defaultSchema)) {
            summary.append(table.schema);
            summary.append('.');
        }
        summary.append(table.name);
        summary.append(NL);
        output.write(summary.toString());
        for (Table child : table.children) {
            outputGroupSummary(child, depth + 1);
        }
    }

    protected void outputDropGroup(Table table) throws IOException {
        StringBuilder sql = new StringBuilder("DROP GROUP IF EXISTS ");
        qualifiedName(table, sql);
        sql.append(";").append(NL);
        output.write(sql.toString());
    }

    protected void outputDropTables(Table parentTable) throws IOException {
        for (Table child : parentTable.children) {
            outputDropTables(child);
        }
        outputDropTable(parentTable);
    }

    protected void outputDropTable(Table table) throws IOException {
        StringBuilder sql = new StringBuilder("DROP TABLE IF EXISTS ");
        qualifiedName(table, sql);
        sql.append(";").append(NL);
        output.write(sql.toString());
        table.dropped = true;
    }

    protected void outputCreateTables(Table rootTable) throws SQLException, IOException {
        PreparedStatement stmt = connection.prepareStatement(
                "SELECT column_name," +
                " type, length, precision, scale," +
                " character_set_name, collation_name, nullable," +
                " sequence_schema, sequence_name, identity_generation" +
                " FROM information_schema.columns WHERE schema_name = ? AND table_name = ? ORDER BY position");
        outputCreateTables(stmt, rootTable);
        stmt.close();
    }

    protected void outputCreateTables(PreparedStatement stmt, Table parentTable) throws SQLException, IOException {
        outputCreateTable(stmt, parentTable);
        for (Table child : parentTable.children) {
            outputCreateTables(stmt, child);
        }
    }
    
    protected void outputCreateTable(PreparedStatement stmt, Table table) throws SQLException, IOException {
        Set<String> pkey = null, gkey = null;
        if (!table.primaryKeys.isEmpty())
            pkey = new HashSet<String>(table.primaryKeys);
        if (table.childKeys != null)
            gkey = new HashSet<String>(table.childKeys);
        StringBuilder sql = new StringBuilder("CREATE TABLE ");
        qualifiedName(table, sql);
        sql.append('(');
        stmt.setString(1, table.schema);
        stmt.setString(2, table.name);
        ResultSet rs = stmt.executeQuery();
        boolean first = true;
        while (rs.next()) {
            if (first) {
                first = false;
            }
            else {
                sql.append(',');
            }
            sql.append(NL).append("  ");
            String column = rs.getString(1);
            identifier(column, sql, false);
            sql.append(' ');
            type(rs.getString(2), rs.getInt(3), rs.getInt(4), rs.getInt(5), sql);
            String charset = rs.getString(6);
            if (!rs.wasNull()) {
                sql.append(" CHARACTER SET ").append(charset);
            }
            String collation = rs.getString(7);
            if (!rs.wasNull()) {
                sql.append(" COLLATE ").append(collation);
            }
            if ("NO".equals(rs.getString(8))) {
                sql.append(" NOT NULL");
            }
            String identityGenerate = rs.getString(11);
            if (!rs.wasNull()) {
                String sequenceSchema = rs.getString(9);
                String sequenceName = rs.getString(10);
                Sequence seq = sequences.get(sequenceSchema).get(sequenceName);
                String generated = " GENERATED " + identityGenerate +
                    " AS IDENTITY (START WITH " + seq.startWith +
                    ", INCREMENT BY " + seq.incrementBy + ")";
                if (dumpData && "ALWAYS".equals(identityGenerate)) {
                    StringBuilder sql2 = new StringBuilder("ALTER TABLE ");
                    qualifiedName(table, sql2);
                    sql2.append(" ALTER COLUMN ");
                    identifier(column, sql2, false);
                    sql2.append(" SET").append(generated).append(";").append(NL);
                    afterDataStatements.add(sql2.toString());
                }
                else {
                    sql.append(generated);
                }
            }
            
            if (pkey != null) {
                pkey.remove(column);
                if (pkey.isEmpty()) {
                    if (table.primaryKeys.size() == 1) {
                        sql.append(" PRIMARY KEY");
                    }
                    else {
                        sql.append(',').append(NL).append("  PRIMARY KEY");
                        keys(table.primaryKeys, sql);
                    }
                    pkey = null;
                }
            }
            if (gkey != null) {
                gkey.remove(column);
                if (gkey.isEmpty()) {
                    sql.append(',').append(NL).append("  GROUPING FOREIGN KEY");
                    keys(table.childKeys, sql);
                    sql.append(" REFERENCES ");
                    qualifiedName(table.parent, sql);
                    keys(table.parentKeys, sql);
                    gkey = null;
                }
            }
        }
        rs.close();
        assert (pkey == null);
        assert (gkey == null);
        sql.append(NL).append(");").append(NL).append(NL);
        output.write(sql.toString());
        table.dumped = true;
    }

    protected void type(String type, int length, int precision, int scale, StringBuilder sql) {
        boolean unsigned = type.endsWith(" unsigned");
        if (unsigned)
            type = type.substring(0, type.length() - 9);
        if (precision > 0) {
            sql.append(type.toUpperCase());
            sql.append('(').append(precision);
            if (scale > 0)
                sql.append(',').append(scale);
            sql.append(')');
        }
        else if ("varchar".equals(type) ||
                 "char".equals(type)) {
            sql.append(type.toUpperCase());
            sql.append('(').append(length).append(')');
        }
        else if ("varbinary".equals(type) ||
                 "binary".equals(type)) {
            sql.append("binary".equals(type) ? "CHAR" : "VARCHAR");
            sql.append('(').append(length).append(')');
            sql.append(" FOR BIT DATA");
        }
        else if ("float".equals(type)) {
            sql.append("REAL");
        }
        else {
            // Fixed-size numerics have a length, but it doesn't mean much.
            sql.append(type.toUpperCase());
        }
        if (unsigned)
            sql.append(" UNSIGNED");
    }
    
    protected void outputCreateIndexes(Table rootTable) throws SQLException, IOException {
        PreparedStatement istmt = connection.prepareStatement("SELECT index_name, is_unique, join_type, index_method FROM information_schema.indexes WHERE schema_name = ? AND table_name = ? AND index_type IN ('INDEX','UNIQUE') ORDER BY index_id");
        PreparedStatement icstmt = connection.prepareStatement("SELECT column_schema_name, column_table_name, column_name, is_ascending FROM information_schema.index_columns WHERE schema_name = ? AND index_table_name = ? AND index_name = ? ORDER BY ordinal_position");
        outputCreateIndexes(istmt, icstmt, rootTable);
        icstmt.close();
        istmt.close();
    }

    protected void outputCreateIndexes(PreparedStatement istmt, PreparedStatement icstmt, Table parentTable) throws SQLException, IOException {
        outputCreateIndexesForTable(istmt, icstmt, parentTable);
        for (Table child : parentTable.children) {
            outputCreateIndexes(istmt, icstmt, child);
        }
    }

    protected void outputCreateIndexesForTable(PreparedStatement istmt, PreparedStatement icstmt, Table table) throws SQLException, IOException {
        istmt.setString(1, table.schema);
        istmt.setString(2, table.name);
        ResultSet rs = istmt.executeQuery();
        while (rs.next()) {
            outputCreateIndex(icstmt, table, rs.getString(1), rs.getString(2), rs.getString(3), rs.getString(4));
        }
        rs.close();
    }    

    protected void outputCreateIndex(PreparedStatement icstmt, Table table, String index, String unique, String joinType, String indexMethod) throws SQLException, IOException {
        StringBuilder sql = new StringBuilder("CREATE ");
        if ("YES".equals(unique))
            sql.append("UNIQUE ");
        sql.append("INDEX ");
        identifier(index, sql, true);
        sql.append(" ON ");
        qualifiedName(table, sql);
        sql.append('(');
        if (indexMethod != null)
            sql.append(indexMethod).append("(");
        icstmt.setString(1, table.schema);
        icstmt.setString(2, table.name);
        icstmt.setString(3, index);
        ResultSet rs = icstmt.executeQuery();
        boolean first = true;
        while (rs.next()) {
            if (first) {
                first = false;
            }
            else {
                sql.append(", ");
            }
            if (joinType != null) {
                // Group index.
                String indexSchema = rs.getString(1);
                String indexTable = rs.getString(2);
                if (!table.schema.equals(indexSchema) ||
                    !table.name.equals(indexTable)) {
                    if (!table.schema.equals(indexSchema)) {
                        identifier(indexSchema, sql, true);
                        sql.append('.');
                        identifier(indexTable, sql, true);
                        sql.append('.');
                    }
                    else {
                        identifier(indexTable, sql, true);
                        sql.append('.');
                    }
                }
            }
            identifier(rs.getString(3), sql, false);
            if ("NO".equals(rs.getString(4))) {
                sql.append(" DESC");
            }
        }
        rs.close();
        if (indexMethod != null)
            sql.append(")");
        sql.append(')');
        if (joinType != null) {
            sql.append(" USING ").append(joinType).append(" JOIN");
        }
        sql.append(';').append(NL);
        output.write(sql.toString());
    }

    protected void dependentViews(Table parentTable, Set<View> views) {
        views.addAll(parentTable.dependedOn);
        for (Table child : parentTable.children) {
            dependentViews(child, views);
        }
    }

    protected void ensureDropViews(Set<View> views) throws IOException {
        for (View view : views) {
            ensureDropView(view);
        }
    }

    protected void ensureDropView(View view) throws IOException {
        if (view.dropped) return;

        ensureDropViews(view.dependedOn);
        outputDropView(view);
        view.dropped = true;
    }

    protected void dumpViews(String schema) throws IOException {
        dumpViews(views.get(schema).values());
    }

    protected void dumpViews(Collection<View> views) throws IOException {
        for (View view : views) {
            dumpView(view);
        }
    }

    protected void dumpView(View view) throws IOException {
        if (view.dumped) return;

        boolean available = true;
        for (Viewed need : view.dependsOn) {
            if (need instanceof View)
                dumpView((View)need);
            if (!need.dumped) {
                available = false;
                break;
            }
        }
        if (!available) return;
        
        ensureDropView(view);
        output.write(view.definition);
        output.write(";"+NL+NL);
        view.dumped = true;
    }

    protected void outputDropView(View view) throws IOException {
        StringBuilder sql = new StringBuilder("DROP VIEW IF EXISTS ");
        qualifiedName(view, sql);
        sql.append(";").append(NL);
        output.write(sql.toString());
    }

    protected void keys(Collection<String> keys, StringBuilder sql) {
        sql.append('(');
        boolean first = true;
        for (String key : keys) {
            if (first) {
                first = false;
            }
            else {
                sql.append(", ");
            }
            identifier(key, sql, false);
        }
        sql.append(')');
    }

    protected void qualifiedName(Named table, StringBuilder sql) {
        if (!table.schema.equals(defaultSchema)) {
            identifier(table.schema, sql, true);
            sql.append('.');
        }
        identifier(table.name, sql, true);
    }

    protected void identifier(String id, StringBuilder sql, boolean caseMatters) {
        if (id.matches((caseMatters) ? "[a-z][_a-z0-9]*" : "[A-Za-z][_A-Za-z0-9]*")) {
            sql.append(id);
        }
        else {
            sql.append('`');
            sql.append(id.replace("`", "``"));
            sql.append('`');
        }
    }

    protected void dumpData(Table rootTable) throws SQLException, IOException {
        StringBuilder sql = new StringBuilder("CALL sys.dump_group('");
        sql.append(rootTable.schema.replace("'", "''"));
        sql.append("','");
        sql.append(rootTable.name.replace("'", "''"));
        sql.append("',");
        sql.append(insertMaxRowCount);
        sql.append(")");
        copyManager.copyOut(sql.toString(), output);
        output.write(NL);
    }

    protected void openOutput() throws Exception {
        if (outputFile != null)
            output = new OutputStreamWriter(new FileOutputStream(outputFile), "UTF-8");
        else
            output = new OutputStreamWriter(System.out);
    }

    protected void openConnection() throws Exception {
        Class.forName(DRIVER_NAME);
        String url = String
            .format("jdbc:postgresql://%s:%d/%s",
                    host, port, (defaultSchema != null) ? defaultSchema : "information_schema");
        connection = DriverManager.getConnection(url, user, password);
        if (dumpData)
            copyManager = new CopyManager((org.postgresql.core.BaseConnection)connection);
    }

    protected void close() {
        if (output != null) {
            try {
                output.close();
            }
            catch (IOException ex) {
            }
            output = null;
        }
        if (connection != null) {
            try {
                connection.close();
            }
            catch (SQLException ex) {
            }
            connection = null;
        }
    }

}
