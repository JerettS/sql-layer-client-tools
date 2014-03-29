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

public class BackslashQuery
{
    private static final String NOT_IN_SYSTEM = " NOT IN ('information_schema', 'security_schema', 'sqlj', 'sys') ";

    private final String base;
    private final String detail;
    private final String from;
    private final String where;
    private final String order;
    private final String schema;

    public final int args;
    public final String descType;
    public final BackslashQuery descQuery;

    private BackslashQuery(String base, String detail, String from, String where, String order, String schema) {
        this(base, detail, from, where, order, schema, null, null);
    }

    private BackslashQuery(String base, String detail, String from, String where, String order, String schema,
                           String descType) {
        this(base, detail, from, where, order, schema, descType, null);
    }

    private BackslashQuery(String base, String detail, String from, String where, String order, String schema,
                           BackslashQuery descQuery) {
        this(base, detail, from, where, order, schema, null, descQuery);
    }

    private BackslashQuery(String base, String detail, String from, String where, String order, String schema,
                           String descType, BackslashQuery descQuery) {
        this.base = base;
        this.detail = detail;
        this.from = from;
        this.where = where;
        this.order = order;
        this.schema = schema;
        this.descType = descType;
        this.descQuery = descQuery;
        int cnt = 0;
        for(int i = 0; i < where.length(); ++i) {
            if(where.charAt(i) == '?') {
                cnt++;
            }
        }
        this.args = cnt;
    }

    public int argCount() {
        return args;
    }

    public String build(boolean isDetail, boolean isSystem) {
        return base +
            (isDetail && (detail != null) ? detail : "") +
            from +
            where +
            (isSystem || (schema == null) ? "" : " AND " + schema + NOT_IN_SYSTEM)
            + order;
    }

    public static final BackslashQuery DESCRIBE_SEQUENCES = new BackslashQuery(
        "SELECT data_type \"Type\", start_value \"Start\", minimum_value \"Min\", maximum_value \"Max\", increment \"Inc\", cycle_option \"Cycle\" ",
        null,
        " FROM information_schema.sequences ",
        " WHERE sequence_schema LIKE ? AND sequence_name LIKE ? ",
        " ORDER BY sequence_schema,sequence_name ",
        " sequence_schema ",
        "Sequence"
    );

    public static final BackslashQuery DESCRIBE_TABLES = new BackslashQuery(
        "SELECT column_name \"Column\", COLUMN_TYPE_STRING(table_schema, table_name, column_name) \"Type\", is_nullable \"Nullable\" ",
        ", column_default \"Default\",  character_set_name \"Charset\", collation_name \"Collation\", CONCAT(sequence_schema, '.', sequence_name) \"Sequence\" ",
        " FROM information_schema.columns ",
        " WHERE table_schema LIKE ? AND table_name LIKE ? ",
        " ORDER BY ordinal_position ",
        " table_schema ",
        "Table"
    );

    public static final BackslashQuery DESCRIBE_VIEWS = new BackslashQuery(
        DESCRIBE_TABLES.base, DESCRIBE_TABLES.detail, DESCRIBE_TABLES.from, DESCRIBE_TABLES.where, DESCRIBE_TABLES.order, DESCRIBE_TABLES.schema,
        "View"
    );

    public static final BackslashQuery EXTRA_TABLE_INDEXES = new BackslashQuery(
        "SELECT IF(i.index_type = 'UNIQUE', 'UNIQUE ', ''), i.index_name, "+
            "GROUP_CONCAT(IF(i.join_type IS NULL, ic.column_name, ic.column_table || '.' || ic.column_name) SEPARATOR ', '), "+
            "IFNULL(' USING ' || i.join_type || ' JOIN', '') ",
        null,
        "FROM information_schema.indexes i INNER JOIN information_schema.index_columns ic "+
            " ON i.table_schema=ic.index_table_schema AND i.table_name=ic.index_table_name AND i.index_name=ic.index_name ",
        "WHERE i.table_schema = ? AND i.table_name= ? ",
        "GROUP BY i.index_name,i.index_type,i.join_type",
        null
    );

    public static final BackslashQuery EXTRA_TABLE_FK_REFERENCES = new BackslashQuery(
        "SELECT rc.constraint_name, GROUP_CONCAT(kcu.column_name SEPARATOR ', '), o_kcu.table_name, GROUP_CONCAT(o_kcu.column_name SEPARATOR ', ') ",
        null,
        "FROM information_schema.referential_constraints rc "+
            "INNER JOIN information_schema.key_column_usage kcu "+
            "  USING(constraint_schema, constraint_name) "+
            "INNER JOIN information_schema.key_column_usage o_kcu "+
            "  ON rc.unique_constraint_schema = o_kcu.constraint_schema "+
            "  AND rc.unique_constraint_name = o_kcu.constraint_name "+
            "  AND kcu.position_in_unique_constraint = o_kcu.ordinal_position ",
        "WHERE rc.constraint_schema = ? AND kcu.table_name = ? ",
        "GROUP BY rc.constraint_name,o_kcu.table_name",
        null
    );

    public static final BackslashQuery EXTRA_TABLE_GFK_REFERENCES = new BackslashQuery(
        EXTRA_TABLE_FK_REFERENCES.base.replace("rc", "gc"),
        null,
        EXTRA_TABLE_FK_REFERENCES.from.replace("rc", "gc")
                                         .replace("referential_constraints", "grouping_constraints")
                                         .replace("unique_constraint_schema", "unique_schema"),
        EXTRA_TABLE_FK_REFERENCES.where.replace("rc", "gc"),
        EXTRA_TABLE_FK_REFERENCES.order.replace("rc", "gc"),
        null
    );

    public static final BackslashQuery EXTRA_TABLE_FK_REFERENCED_BY = new BackslashQuery(
        "SELECT kcu.table_name, rc.constraint_name, GROUP_CONCAT(kcu.column_name SEPARATOR ', '), o_kcu.table_name, GROUP_CONCAT(o_kcu.column_name SEPARATOR ', ') ",
        null,
        "FROM information_schema.table_constraints tc "+
            "INNER JOIN information_schema.referential_constraints rc "+
            "  ON tc.table_schema = rc.unique_constraint_schema"+
            " AND tc.constraint_name = rc.unique_constraint_name "+
            "INNER JOIN information_schema.key_column_usage kcu "+
            " ON rc.constraint_schema = kcu.constraint_schema "+
            " AND rc.constraint_name = kcu.constraint_name "+
            "INNER JOIN information_schema.key_column_usage o_kcu "+
            " ON rc.unique_constraint_schema = o_kcu.constraint_schema "+
            " AND rc.unique_constraint_name = o_kcu.constraint_name "+
            " AND kcu.position_in_unique_constraint = o_kcu.ordinal_position ",
        "WHERE tc.table_schema = ? AND tc.table_name = ? ",
        "GROUP BY kcu.table_name,rc.constraint_schema,rc.constraint_name,o_kcu.table_name",
        null
    );

    public static final BackslashQuery EXTRA_TABLE_GFK_REFERENCED_BY = new BackslashQuery(
        EXTRA_TABLE_FK_REFERENCED_BY.base.replace("rc", "gc"),
        null,
        EXTRA_TABLE_FK_REFERENCED_BY.from.replace("rc", "gc")
                                         .replace("referential_constraints", "grouping_constraints")
                                         .replace("unique_constraint_schema", "unique_schema"),
        EXTRA_TABLE_FK_REFERENCED_BY.where.replace("rc", "gc"),
        EXTRA_TABLE_FK_REFERENCED_BY.order.replace("rc", "gc"),
        null
    );


    public static final BackslashQuery LIST_INDEXES = new BackslashQuery(
        "SELECT table_schema \"Schema\", table_name \"Table\", index_name \"Name\" ",
        ", is_unique \"Unique\", join_type \"Join Type\", index_method \"Index Method\", storage_name \"Storage Name\" ",
        " FROM information_schema.indexes ",
        " WHERE table_schema LIKE ? AND table_name LIKE ? AND index_name LIKE ? ",
        " ORDER BY table_schema,table_name,index_name ",
        " table_schema "
    );

    public static final BackslashQuery LIST_SEQUENCES = new BackslashQuery(
        "SELECT sequence_schema \"Schema\", sequence_name \"Name\" ",
        ", storage_name \"Storage Name\" ",
        " FROM information_schema.sequences ",
        " WHERE sequence_schema LIKE ? AND sequence_name LIKE ? ",
        " ORDER BY sequence_schema,sequence_name ",
        " sequence_schema ",
        DESCRIBE_SEQUENCES
    );

    public static final BackslashQuery LIST_SCHEMAS = new BackslashQuery(
        "SELECT schema_name \"Name\" ",
        null,
        " FROM information_schema.schemata ",
        " WHERE schema_name LIKE ? ",
        " ORDER BY schema_name ",
        " schema_name "
    );

    public static final BackslashQuery LIST_TABLES = new BackslashQuery(
        "SELECT table_schema \"Schema\", table_name \"Name\" ",
        ", table_type \"Type\", table_id \"ID\", storage_name \"Storage Name\" ",
        " FROM information_schema.tables ",
        " WHERE table_schema LIKE ? AND table_name LIKE ? AND table_type LIKE '%TABLE'",
        " ORDER BY table_schema,table_name ",
        " table_schema ",
        DESCRIBE_TABLES
    );

    public static final BackslashQuery LIST_VIEWS = new BackslashQuery(
        "SELECT table_schema \"Schema\", table_name \"Name\" ",
        ", table_type \"Type\" ",
        " FROM information_schema.tables ",
        " WHERE table_schema LIKE ? AND table_name LIKE ? AND table_type LIKE '%VIEW'",
        " ORDER BY table_schema,table_name ",
        " table_schema ",
        DESCRIBE_VIEWS
    );

    public static final BackslashQuery LIST_ALL = new BackslashQuery(
        "SELECT * ",
        null,
        " FROM (" +
            LIST_TABLES.base + ", table_type \"Type\" " + LIST_TABLES.from + " UNION " +
            LIST_VIEWS.base + ", table_type \"Type\" " + LIST_VIEWS.from + " UNION " +
            LIST_SEQUENCES.base +", 'SEQUENCE' " + LIST_SEQUENCES.from + ") sub ",
        "WHERE \"Schema\" LIKE ? AND \"Name\" LIKE ? ",
        " ORDER BY \"Schema\", \"Name\", \"Type\" ",
        "\"Schema\"",
        null,
        null
    );
}