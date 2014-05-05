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

package com.foundationdb.sql.client.dump;

import org.postgresql.copy.CopyManager;

import java.io.*;
import java.sql.*;
import java.util.*;

public class DumpClient
{
    private static final String PROGRAM_NAME = "fdbsqldump";
    private static final String NL = System.getProperty("line.separator");

    private DumpClientOptions options;
    private boolean dumpSchema = true;
    private boolean dumpData = true;
    private Map<String,Map<String,Table>> schemas = new TreeMap<String,Map<String,Table>>();
    private Map<String, Map<String,Sequence>> sequences = new TreeMap<String,Map<String, Sequence>>();
    private Map<String,Map<String,View>> views = new TreeMap<>();
    private Queue<String> afterDataStatements = new ArrayDeque<String>();
    private String defaultSchema = null;
    private Writer output;
    private Connection connection;
    private CopyManager copyManager;



    public static void main(String[] args) throws Exception {
        DumpClientOptions options = new DumpClientOptions();
        options.parseOrDie(PROGRAM_NAME, args);
        DumpClient dumpClient = new DumpClient(options);
        try {
            dumpClient.dump();
        } catch (Exception e) {
            System.err.println(e.getMessage());
            System.exit(1);
        }
        finally {
            dumpClient.close();
        }
    }

    public DumpClient() {
    }

    public DumpClient(DumpClientOptions options) {
        this.options = options;
        this.dumpSchema = !options.noSchemas;
        this.dumpData = !options.noData;
        if (options.commitFrequency == null) {
            options.commitFrequency = 0L;
        }
        for (String schema : options.schemas) {
            addSchema(schema);
        }
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
            loadForeignKeys();
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
            if (dumpSchema && dumpData) {
                // Add foreign keys after all the data so that references are satisfied.
                for (String schema : schemas.keySet()) {
                    for (Table table : schemas.get(schema).values()) {
                        if (table.parent == null) {
                            outputAddForeignKeys(table);
                        }
                    }
                }
            }
        }
        close();
    }
    
    protected void loadTables(String schema) throws SQLException {
        Map<String,Table> tables = schemas.get(schema);
        PreparedStatement stmt = connection.prepareStatement("SELECT QUOTE_IDENT(table_schema, '`'), table_name, QUOTE_IDENT(table_name, '`') FROM information_schema.tables "+
                                                             "WHERE table_schema = ? AND table_type = 'TABLE' ORDER BY table_id");
        stmt.setString(1, schema);
        ResultSet rs = stmt.executeQuery();
        while (rs.next()) {
            String quotedSchema = rs.getString(1);
            String name = rs.getString(2);
            tables.put(name, new Table(schema, quotedSchema, name, rs.getString(3)));
        }
        stmt.close();
    }

    protected void loadSequences (String schema) throws SQLException {
        Map<String, Sequence> seqs = sequences.get(schema);
        PreparedStatement stmt = connection.prepareStatement(
                "select QUOTE_IDENT(s.sequence_schema, '`'), s.sequence_name, QUOTE_IDENT(s.sequence_name, '`')," +
                " nextval(s.sequence_schema, s.sequence_name), " +
                " s.increment, s.minimum_value, s.maximum_value, " +
                " s.cycle_option = 'YES', " +
                " c.sequence_name is not null" +
                " from information_schema.sequences s " +
                " left join information_schema.columns c on "+
                "   s.sequence_schema=c.sequence_schema and s.sequence_name=c.sequence_name" +
                " where s.sequence_schema = ? order by s.sequence_name");
        stmt.setString(1, schema);
        ResultSet rs = stmt.executeQuery();
        while (rs.next()) {
            String quotedSchema = rs.getString(1);
            String name = rs.getString(2);
            seqs.put(name, new Sequence (schema, quotedSchema, name, rs.getString(3),
                                         rs.getLong(4), rs.getLong(5), rs.getLong(6), rs.getLong(7),
                                         rs.getBoolean(8), rs.getBoolean(9)));
        }
    }

    protected void loadGroups(String schema, Deque<String> pending) throws SQLException {
        PreparedStatement kstmt = connection.prepareStatement("SELECT QUOTE_IDENT(column_name, '`') FROM information_schema.key_column_usage WHERE table_schema = ? AND table_name = ? AND constraint_name = ? ORDER BY ordinal_position");
        PreparedStatement stmt = connection.prepareStatement("SELECT c.constraint_schema, QUOTE_IDENT(c.constraint_schema, '`'), c.constraint_table_name, QUOTE_IDENT(c.constraint_table_name, '`'), p.table_schema, "+
                                                             "QUOTE_IDENT(p.table_schema, '`'), p.table_name, QUOTE_IDENT(p.table_name, '`'), c.constraint_name, c.unique_constraint_name, p.constraint_type = 'PRIMARY KEY'"+
                                                             "FROM information_schema.grouping_constraints c "+
                                                             "LEFT JOIN information_schema.table_constraints p "+
                                                             "  ON  c.unique_schema = p.constraint_schema "+
                                                             "  AND c.unique_constraint_name = p.constraint_name "+
                                                             "WHERE p.table_name IS NOT NULL AND (c.constraint_schema = ? OR c.unique_schema = ?)");
        stmt.setString(1, schema);
        stmt.setString(2, schema);
        ResultSet rs = stmt.executeQuery();
        while (rs.next()) {
            Table child = findOrCreateTable(rs.getString(1), rs.getString(2), rs.getString(3), rs.getString(4), pending);
            Table parent = findOrCreateTable(rs.getString(5), rs.getString(6), rs.getString(7), rs.getString(8), pending);
            child.parent = parent;
            parent.children.add(child);
            child.childKeys = loadKeys(kstmt, child.schema, child.name, rs.getString(9));
            boolean parentIsPrimary = rs.getBoolean(11);
            List<String> keys = null;
            if (parentIsPrimary)
                keys = parent.primaryKeys;
            if (keys == null)
                keys = loadKeys(kstmt, parent.schema, parent.name, rs.getString(10));
            child.parentKeys = keys;
            if ((parent.primaryKeys == null) && parentIsPrimary)
                parent.primaryKeys = keys;
        }
        stmt.close();
        for (Table table : schemas.get(schema).values()) {
            if (table.primaryKeys == null) {
                table.primaryKeys = loadKeys(kstmt, table.schema, table.name, table.name+".PRIMARY");
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
        PreparedStatement stmt = connection.prepareStatement("SELECT QUOTE_IDENT(table_schema, '`'), table_name, QUOTE_IDENT(table_name, '`'), view_definition "+
                                                             "FROM information_schema.views WHERE table_schema = ? ORDER BY table_name");
        for (Map.Entry<String,Map<String,View>> entry : views.entrySet()) {
            String schema = entry.getKey();
            Map<String,View> views = entry.getValue();
            stmt.setString(1, schema);
            ResultSet rs = stmt.executeQuery();
            while (rs.next()) {
                String quotedSchema = rs.getString(1);
                String name = rs.getString(2);
                String quotedName = rs.getString(3);
                views.put(name, new View(schema, quotedSchema, name, quotedName, rs.getString(4)));
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

    protected void loadForeignKeys() throws SQLException {
        PreparedStatement stmt = connection.prepareStatement("SELECT kcu1.position_in_unique_constraint," +
                " rc.constraint_name, QUOTE_IDENT(rc.constraint_name, '`'), rc.match_option, rc.update_rule, rc.delete_rule, kcu1.table_schema, " +
                "QUOTE_IDENT(kcu1.table_schema, '`'), kcu1.table_name, QUOTE_IDENT(kcu1.table_name, '`')," +
                " kcu1.column_name, kcu2.table_schema, QUOTE_IDENT(kcu2.table_schema, '`'), kcu2.table_name, " +
                "QUOTE_IDENT(kcu2.table_schema, '`'), kcu2.column_name FROM information_schema.referential_constraints rc" +
                " INNER JOIN information_schema.key_column_usage kcu1 USING (constraint_schema, constraint_name) " +
                "INNER JOIN information_schema.key_column_usage kcu2 ON rc.unique_constraint_schema = kcu2.constraint_schema " +
                "AND rc.unique_constraint_name = kcu2.constraint_name " +
                "AND kcu1.position_in_unique_constraint = kcu2.ordinal_position " +
                "WHERE kcu1.table_schema = ? OR (kcu2.table_schema = ? AND kcu1.table_schema <> kcu2.table_schema)");
        ForeignKey fkey = null;
        Set<String> schemas = this.schemas.keySet();
        for (String schema : schemas) {
            stmt.setString(1, schema);
            stmt.setString(2, schema);
            ResultSet rs = stmt.executeQuery();
            while (rs.next()) {
                int pos = rs.getInt(1);
                String name = rs.getString(2);
                String quotedName = rs.getString(3);
                String match = rs.getString(4);
                String update = rs.getString(5);
                String delete = rs.getString(6);
                String referencingSchema = rs.getString(7);
                String quotedReferencingSchema = rs.getString(8);
                String referencingTable = rs.getString(9);
                String quotedReferencingTable = rs.getString(10);
                String referencingColumn = rs.getString(11);
                String referencedSchema = rs.getString(12);
                String quotedReferencedSchema = rs.getString(13);
                String referencedTable = rs.getString(14);
                String quotedReferencedTable = rs.getString(15);
                String referencedColumn = rs.getString(16);
                if (!schema.equals(referencingSchema) &&
                    schemas.contains(referencedSchema))
                    continue; // Will get to it then.
                assert name.startsWith(referencingTable + ".");
                // TODO: Broken, remove when constraint names are accurate from the sql-layer
                name = name.substring(referencingTable.length() + 1);

                if (quotedName.startsWith("`") ) {
                    quotedName = "`" + quotedName.substring(referencingTable.length() + 2);
                }
                else {
                    quotedName = quotedName.substring(referencingTable.length() + 1);
                }

                if (pos == 0) {
                    fkey = new ForeignKey(findOrCreateTable(referencingSchema, quotedReferencingSchema, referencingTable, quotedReferencingTable,  null),
                                          findOrCreateTable(referencedSchema, quotedReferencedSchema, referencedTable, quotedReferencedTable, null),
                                          name, quotedName, match, update, delete);
                }
                else {
                    assert ((fkey != null) &&
                            name.equals(fkey.name) &&
                            (pos == fkey.referencingColumns.size()) &&
                            referencingSchema.equals(fkey.referencingTable.schema) &&
                            referencingTable.equals(fkey.referencingTable.name) &&
                            referencedSchema.equals(fkey.referencedTable.schema) &&
                            referencedTable.equals(fkey.referencedTable.name));
                }
                fkey.referencingColumns.add(referencingColumn);
                fkey.referencedColumns.add(referencedColumn);
            }
        }
        stmt.close();
    }

    protected static class Named implements Comparable<Named> {
        String schema, quotedSchema, name, quotedName;
        
        public Named(String schema, String quotedSchema, String name, String quotedName) {
            this.schema = schema;
            this.quotedSchema = quotedSchema;
            this.name = name;
            this.quotedName = quotedName;
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
        
        public Viewed(String schema, String quotedSchema, String name, String quotedName) {
            super(schema, quotedSchema, name, quotedName);
        }
    }

    protected static class Table extends Viewed {
        Table parent;
        List<Table> children = new ArrayList<Table>();
        List<String> primaryKeys, childKeys, parentKeys;
        Set<ForeignKey> foreignKeys = new TreeSet<ForeignKey>();
        
        public Table(String schema, String quotedSchema, String name, String quotedName) {
            super(schema, quotedSchema, name, quotedName);
        }
    }
    
    protected static class View extends Viewed {
        String definition;
        Set<Viewed> dependsOn = new TreeSet<Viewed>();
        
        public View(String schema, String quotedSchema, String name, String quotedName, String definition) {
            super(schema, quotedSchema, name, quotedName);
            this.definition = definition;
        }
    }

    protected static class Sequence extends Named {
        long startWith, incrementBy;
        long minValue, maxValue;
        boolean cycle;
        boolean identity;
        
        public Sequence (String schema, String quotedSchema, String name, String quotedName,
                long startWith, long incrementBy,
                long minValue, long maxValue,
                boolean cycle, boolean identity) {
            super(schema, quotedSchema, name, quotedName);
            this.startWith = startWith;
            this.incrementBy = incrementBy;
            this.minValue = minValue;
            this.maxValue = maxValue;
            this.cycle = cycle;
            this.identity = identity;
        }
    }

    protected static class ForeignKey implements Comparable<ForeignKey> {
        Table referencingTable, referencedTable;
        String name, quotedName, match, update, delete;
        List<String> referencingColumns, referencedColumns;
        boolean dumped;

        public ForeignKey(Table referencingTable, Table referencedTable,
                          String name, String quotedName, String match, String update, String delete) {
            this.referencingTable = referencingTable;
            this.referencedTable = referencedTable;
            this.name = name;
            this.quotedName = quotedName;
            this.match = match;
            this.update = update;
            this.delete = delete;
            this.referencingColumns = new ArrayList<String>();
            this.referencedColumns = new ArrayList<String>();
            referencingTable.foreignKeys.add(this);
            referencedTable.foreignKeys.add(this);
        }
        
        @Override
        public int compareTo(ForeignKey o) {
            int cmp = referencingTable.compareTo(o.referencingTable);
            return cmp == 0 ? name.compareTo(o.name) : cmp;
        }
    }

    protected Table findOrCreateTable(String schema, String quotedSchema, String name, String quotedName,
                                      Deque<String> pending) {
        Map<String,Table> tables = schemas.get(schema);
        if ((tables == null) && (pending != null)) {
            // This is the odd case of a reference to some table not in a schema that
            // was requested. Have to get that schema anyway.
            tables = new TreeMap<String,Table>();
            schemas.put(schema, tables);
            pending.addLast(schema);
        }
        Table table = tables.get(name);
        if (table == null) {
            table = new Table(schema, quotedSchema, name, quotedName);
            if (tables != null)
                tables.put(name, table);
            else
                // A table only referenced by foreign keys.
                table.dropped = table.dumped = true;
        }
        return table;
    }
    
    protected void dumpSequences(String schema) throws IOException {
        StringBuilder sql = new StringBuilder();
        
        // drop sequences
        for (Sequence seq : sequences.get(schema).values()) {
            if (seq.identity) {
                continue;
            }
            sql.append ("DROP SEQUENCE IF EXISTS ");
            qualifiedName(seq, sql);
            sql.append(" RESTRICT");
            sql.append(";").append(NL);
        }
        if (sql.length() > 0) {
            sql.append(NL);
        }
        // create sequences
        for (Sequence seq : sequences.get(schema).values()) {
            if (seq.identity) {
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
        if (sql.length() > 0) {
            sql.append(NL).append(NL);
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
            outputDropForeignKeys(table, table);
            if (!table.children.isEmpty())
                outputDropGroup(table);
            outputDropTables(table);
            output.write(NL);
            outputCreateTables(table);
            output.write(NL);
            if (!dumpData)
                outputAddForeignKeys(table);
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
        if (table.dropped) return;

        StringBuilder sql = new StringBuilder("DROP TABLE IF EXISTS ");
        qualifiedName(table, sql);
        sql.append(";").append(NL);
        output.write(sql.toString());
        table.dropped = true;
    }

    protected void outputCreateTables(Table rootTable) throws SQLException, IOException {
        PreparedStatement stmt = connection.prepareStatement(
                "SELECT column_name, QUOTE_IDENT(column_name, '`'), " +
                " data_type, character_maximum_length, numeric_precision, numeric_scale," +
                " character_set_name, collation_name, is_nullable," +
                " sequence_schema, sequence_name, identity_generation" +
                " FROM information_schema.columns WHERE table_schema = ? AND table_name = ? ORDER BY ordinal_position");
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
            String quotedColumn = rs.getString(2);
            sql.append(quotedColumn);
            sql.append(' ');
            type(rs.getString(3), rs.getInt(4), rs.getInt(5), rs.getInt(6), sql);
            String charset = rs.getString(7);
            if (!rs.wasNull()) {
                sql.append(" CHARACTER SET ").append(charset);
            }
            String collation = rs.getString(8);
            if (!rs.wasNull()) {
                sql.append(" COLLATE ").append(collation);
            }
            if ("NO".equals(rs.getString(9))) {
                sql.append(" NOT NULL");
            }
            String identityGenerate = rs.getString(12);
            if (!rs.wasNull()) {
                String sequenceSchema = rs.getString(10);
                String sequenceName = rs.getString(11);
                Sequence seq = sequences.get(sequenceSchema).get(sequenceName);
                String generated = " GENERATED " + identityGenerate +
                    " AS IDENTITY (START WITH " + seq.startWith +
                    ", INCREMENT BY " + seq.incrementBy + ")";
                if (dumpData && "ALWAYS".equals(identityGenerate)) {
                    StringBuilder sql2 = new StringBuilder("ALTER TABLE ");
                    qualifiedName(table, sql2);
                    sql2.append(" ALTER COLUMN ");
                    sql2.append(quotedColumn);
                    sql2.append(" SET").append(generated).append(";").append(NL);
                    afterDataStatements.add(sql2.toString());
                }
                else {
                    sql.append(generated);
                }
            }
            
            if (pkey != null) {
                pkey.remove(quotedColumn);
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
                gkey.remove(quotedColumn);
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
        boolean unsigned = type.endsWith(" UNSIGNED");
        if (unsigned)
            type = type.substring(0, type.length() - 9);
        if (precision > 0) {
            sql.append(type.toUpperCase());
            sql.append('(').append(precision);
            if (scale > 0)
                sql.append(',').append(scale);
            sql.append(')');
        }
        else if ("VARCHAR".equals(type) ||
                 "CHAR".equals(type)) {
            sql.append(type.toUpperCase());
            sql.append('(').append(length).append(')');
        }
        else if ("VARBINARY".equals(type) ||
                 "BINARY".equals(type)) {
            sql.append("BINARY".equals(type) ? "CHAR" : "VARCHAR");
            sql.append('(').append(length).append(')');
            sql.append(" FOR BIT DATA");
        }
        else if ("FLOAT".equals(type)) {
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
        PreparedStatement istmt = connection.prepareStatement("SELECT index_name, QUOTE_IDENT(index_name, '`')," +
                " is_unique, join_type, index_method FROM information_schema.indexes " +
                "WHERE table_schema = ? AND table_name = ? AND index_type IN ('INDEX','UNIQUE') " +
                "ORDER BY index_id");
        PreparedStatement icstmt = connection.prepareStatement("SELECT column_schema, QUOTE_IDENT(column_schema, '`'), " +
                "column_table, QUOTE_IDENT(column_table, '`'), column_name, QUOTE_IDENT(column_name, '`'), is_ascending " +
                "FROM information_schema.index_columns " +
                "WHERE column_schema = ? AND index_table_name = ? AND index_name = ? " +
                "ORDER BY ordinal_position");
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
            outputCreateIndex(icstmt, table, rs.getString(1), rs.getString(2), rs.getString(3), rs.getString(4), rs.getString(5));
        }
        rs.close();
    }    

    protected void outputCreateIndex(PreparedStatement icstmt, Table table, String index, String quotedIndex, String unique, String joinType, String indexMethod) throws SQLException, IOException {
        for (ForeignKey fkey : table.foreignKeys) {
            if ((fkey.referencingTable == table) &&
                index.equals(fkey.name)) {
                return;         // Implicit in FOREIGN KEY definition.
            }
        }
        StringBuilder sql = new StringBuilder("CREATE ");
        if ("YES".equals(unique))
            sql.append("UNIQUE ");
        sql.append("INDEX ");
        sql.append(quotedIndex);
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
                String quotedIndexSchema = rs.getString(2);
                String indexTable = rs.getString(3);
                String quotedIndexTable = rs.getString(4);
                if (!table.schema.equals(indexSchema) ||
                    !table.name.equals(indexTable)) {
                    if (!table.schema.equals(indexSchema)) {
                        sql.append(quotedIndexSchema);
                        sql.append('.');
                        sql.append(quotedIndexTable);
                        sql.append('.');
                    }
                    else {
                        sql.append(quotedIndexTable);
                        sql.append('.');
                    }
                }
            }
            sql.append(rs.getString(6));
            if ("NO".equals(rs.getString(7))) {
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

    protected void outputDropForeignKeys(Table parentTable, Table rootTable) throws IOException {
        for (ForeignKey fkey : parentTable.foreignKeys) {
            if ((parentTable == fkey.referencedTable) &&
                !fkey.referencingTable.dropped &&
                !sameGroup(fkey.referencingTable, rootTable)) {
                outputDropForeignKey(fkey);
            }
        }
        for (Table child : parentTable.children) {
            outputDropForeignKeys(child, rootTable);
        }        
    }

    protected boolean sameGroup(Table table, Table rootTable) {
        while (table != null) {
            if (table == rootTable)
                return true;
            table = table.parent;
        }
        return false;
    }

    protected void outputDropForeignKey(ForeignKey fkey) throws IOException {
        // Two possible strategies:
        // (1) drop the referencing table(s) ahead of their normal order.
        // (2) drop the foreign key by itself.
        // The first is preferred, but not possible in the case of circularities and
        // grouping.
        Deque<Table> toDrop = new ArrayDeque<Table>();
        if (gatherDropForeignKey(fkey.referencingTable, toDrop)) {
            for (Table table : toDrop) {
                outputDropTable(table);
            }
            return;
        }
        // Unfortunately, this form does not have an IF EXISTS form, so an error
        // needs to be ignored.
        StringBuilder sql = new StringBuilder();
        sql.append("-- IGNORE ERRORS").append(NL);
        sql.append("ALTER TABLE ");
        qualifiedName(fkey.referencingTable, sql);
        sql.append(" DROP FOREIGN KEY ");
        sql.append(fkey.quotedName);
        sql.append(';').append(NL);
        output.write(sql.toString());
    }

    protected boolean gatherDropForeignKey(Table table, Deque<Table> toDrop) {
        if (toDrop.contains(table))
            return false;       // Circularity.
        if ((table.parent != null) && !table.children.isEmpty())
            return false;       // Grouping.
        toDrop.addFirst(table);
        for (ForeignKey fkey : table.foreignKeys) {
            if ((table == fkey.referencedTable) &&
                !fkey.referencingTable.dropped &&
                !gatherDropForeignKey(fkey.referencingTable, toDrop)) {
                return false;
            }
        }
        return true;
    }

    protected void outputAddForeignKeys(Table parentTable) throws IOException {
        for (ForeignKey fkey : parentTable.foreignKeys) {
            if (fkey.referencingTable.dumped && fkey.referencedTable.dumped &&
                !fkey.dumped) {
                outputAddForeignKey(fkey);
            }
        }
        for (Table child : parentTable.children) {
            outputAddForeignKeys(child);
        }        
    }

    protected void outputAddForeignKey(ForeignKey fkey) throws IOException {
        StringBuilder sql = new StringBuilder("ALTER TABLE ");
        qualifiedName(fkey.referencingTable, sql);
        sql.append(" ADD CONSTRAINT ");
        sql.append(fkey.quotedName);
        sql.append(" FOREIGN KEY");
        keys(fkey.referencingColumns, sql);
        sql.append(" REFERENCES ");
        qualifiedName(fkey.referencedTable, sql);
        keys(fkey.referencedColumns, sql);
        if (!"NONE".equals(fkey.match))
            sql.append(" MATCH ").append(fkey.match);
        if (!"NO ACTION".equals(fkey.update))
            sql.append(" ON UPDATE ").append(fkey.update);
        if (!"NO ACTION".equals(fkey.delete))
            sql.append(" ON DELETE ").append(fkey.delete);
        sql.append(';').append(NL);
        output.write(sql.toString());
        fkey.dumped = true;
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
        StringBuilder sql =  new StringBuilder("DROP VIEW IF EXISTS ");
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
            sql.append(key);
        }
        sql.append(')');
    }

    protected void qualifiedName(Named table, StringBuilder sql) {
        if (!table.schema.equals(defaultSchema)) {
            sql.append(table.quotedSchema);
            sql.append('.');
        }
        sql.append(table.quotedName);
    }

    protected void dumpData(Table rootTable) throws SQLException, IOException {
        StringBuilder sql = new StringBuilder("CALL sys.dump_group('");
        sql.append(rootTable.schema.replace("'", "''"));
        sql.append("','");
        sql.append(rootTable.name.replace("'", "''"));
        sql.append("',");
        sql.append(options.insertMaxRowCount);
        if (options.commitFrequency > 0) {
            sql.append(",");
            sql.append(options.commitFrequency);
        }
        sql.append(")");
        copyManager.copyOut(sql.toString(), output);
        output.write(NL);
    }

    protected void openOutput() throws Exception {
        if (options.outputFile != null)
            output = new OutputStreamWriter(new FileOutputStream(options.outputFile), "UTF-8");
        else
            output = new OutputStreamWriter(System.out);
    }

    protected void openConnection() throws Exception {
        String url = options.getURL((defaultSchema != null) ? defaultSchema : "information_schema");
        connection = DriverManager.getConnection(url, options.user, options.password);
        if (options.commitFrequency == DumpClientOptions.COMMIT_AUTO) {
            Statement stmt = connection.createStatement();
            stmt.execute("SET transactionPeriodicallyCommit TO 'true'");
            stmt.close();
        }
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
