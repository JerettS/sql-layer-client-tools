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

public class BackslashQueries
{
    private static final String NOT_IN_SYSTEM = " NOT IN ('information_schema', 'security_schema', 'sqlj', 'sys') ";

    private static final String DTV_BASE = "SELECT column_name \"Column\", COLUMN_TYPE_STRING(table_schema, table_name, column_name) \"Type\", is_nullable \"Nullable\" ";
    private static final String DTV_DETAIL = ", column_default \"Default\",  character_set_name \"Charset\", collation_name \"Collation\", CONCAT(sequence_schema, '.', sequence_name) \"Sequence\" ";
    private static final String DTV_FROM = " FROM information_schema.columns ";
    private static final String DTV_WHERE = " WHERE table_schema LIKE ? AND table_name LIKE ? ";
    private static final String DTV_ORDER = " ORDER BY ordinal_position ";
    private static final String DTV_SCHEMA = " table_schema ";

    public static final String DT_INDEXES_BASE = "SELECT IF(i.index_type = 'UNIQUE', 'UNIQUE ', ''), i.index_name, group_concat(ic.column_name) ";
    public static final String DT_INDEXEES_FROM = "FROM information_schema.indexes i INNER JOIN information_schema.index_columns ic "+
                                                  " ON i.table_schema=ic.index_table_schema AND i.table_name=ic.index_table_name AND i.index_name=ic.index_name ";
    public static final String DT_INDEXES_WHERE = "WHERE i.table_schema = ? AND i.table_name= ? ";
    public static final String DT_INDEXES_ORDER = "GROUP BY i.index_name,i.index_type ORDER BY i.index_name ";

    public static final String DT_FK_REFERENCES_BASE = "SELECT rc.constraint_name, GROUP_CONCAT(kcu.column_name), o_kcu.table_name, GROUP_CONCAT(o_kcu.column_name) ";
    public static final String DT_FK_REFERENCES_FROM = "FROM information_schema.referential_constraints rc "+
                                                       "INNER JOIN information_schema.key_column_usage kcu "+
                                                       "  USING(constraint_schema, constraint_name) "+
                                                       "INNER JOIN information_schema.key_column_usage o_kcu "+
                                                       "  ON rc.unique_constraint_schema = o_kcu.constraint_schema "+
                                                       "  AND rc.unique_constraint_name = o_kcu.constraint_name "+
                                                       "  AND kcu.position_in_unique_constraint = o_kcu.ordinal_position ";
    public static final String DT_FK_REFERENCES_WHERE = "WHERE rc.constraint_schema = ? AND kcu.table_name = ? ";
    public static final String DT_FK_REFERENCES_ORDER = "GROUP BY rc.constraint_name,o_kcu.table_name";

    public static final String DT_GFK_REFERENCES_BASE = DT_FK_REFERENCES_BASE.replace("rc", "gc");
    public static final String DT_GFK_REFERENCES_FROM = DT_FK_REFERENCES_FROM.replace("rc", "gc")
                                                                             .replace("referential_constraints", "grouping_constraints")
                                                                             .replace("unique_constraint_schema", "unique_schema");
    public static final String DT_GFK_REFERENCES_WHERE = DT_FK_REFERENCES_WHERE.replace("rc", "gc");
    public static final String DT_GFK_REFERENCES_ORDER = DT_FK_REFERENCES_ORDER.replace("rc", "gc");

    public static final String DT_FK_REFERENCED_BY_BASE = "SELECT kcu.table_name, rc.constraint_name, GROUP_CONCAT(kcu.column_name), GROUP_CONCAT(o_kcu.column_name) ";
    public static final String DT_FK_REFERENCED_BY_FROM = "FROM information_schema.table_constraints tc "+
                                                          "INNER JOIN information_schema.referential_constraints rc "+
                                                          "  ON tc.table_schema = rc.unique_constraint_schema"+
                                                          " AND tc.constraint_name = rc.unique_constraint_name "+
                                                          "INNER JOIN information_schema.key_column_usage kcu "+
                                                          " ON rc.constraint_schema = kcu.constraint_schema "+
                                                          " AND rc.constraint_name = kcu.constraint_name "+
                                                          "INNER JOIN information_schema.key_column_usage o_kcu "+
                                                          " ON rc.unique_constraint_schema = o_kcu.constraint_schema "+
                                                          " AND rc.unique_constraint_name = o_kcu.constraint_name "+
                                                          " AND kcu.position_in_unique_constraint = o_kcu.ordinal_position ";
    public static final String DT_FK_REFERENCED_BY_WHERE = "WHERE tc.table_schema = ? AND tc.table_name = ? ";
    public static final String DT_FK_REFERENCED_BY_ORDER = "GROUP BY kcu.table_name,rc.constraint_schema,rc.constraint_name";


    public static final String DT_GFK_REFERENCED_BY_BASE = DT_FK_REFERENCED_BY_BASE.replace("rc", "gc");
    public static final String DT_GFK_REFERENCED_BY_FROM = DT_FK_REFERENCED_BY_FROM.replace("rc", "gc")
                                                                                   .replace("referential_constraints", "grouping_constraints")
                                                                                   .replace("unique_constraint_schema", "unique_schema");
    public static final String DT_GFK_REFERENCED_BY_WHERE = DT_FK_REFERENCED_BY_WHERE.replace("rc", "gc");
    public static final String DT_GFK_REFERENCED_BY_ORDER = DT_FK_REFERENCED_BY_ORDER.replace("rc", "gc");


    private static final String DQ_BASE = "SELECT data_type \"Type\", start_value \"Start\", minimum_value \"Min\", maximum_value \"Max\", increment \"Inc\", cycle_option \"Cycle\" ";
    private static final String DQ_DETAIL = " ";
    private static final String DQ_FROM = " FROM information_schema.sequences ";
    private static final String DQ_WHERE = " WHERE sequence_schema LIKE ? AND sequence_name LIKE ? ";
    private static final String DQ_ORDER= " ORDER BY sequence_schema,sequence_name ";
    private static final String DQ_SCHEMA = " sequence_schema ";

    private static final String LI_BASE = "SELECT table_schema \"Schema\", table_name \"Table\", index_name \"Name\" ";
    private static final String LI_DETAIL = ", is_unique \"Unique\", join_type \"Join Type\", index_method \"Index Method\", storage_name \"Storage Name\" ";
    private static final String LI_FROM = " FROM information_schema.indexes ";
    private static final String LI_WHERE = " WHERE table_schema LIKE ? AND table_name LIKE ? AND index_name LIKE ? ";
    private static final String LI_ORDER = " ORDER BY table_schema,table_name,index_name ";
    private static final String LI_SCHEMA = " table_schema ";

    private static final String LQ_BASE = "SELECT sequence_schema \"Schema\", sequence_name \"Name\" ";
    private static final String LQ_DETAIL = ", storage_name \"Storage Name\" ";
    private static final String LQ_FROM = " FROM information_schema.sequences ";
    private static final String LQ_WHERE = " WHERE sequence_schema LIKE ? AND sequence_name LIKE ? ";
    private static final String LQ_ORDER= " ORDER BY sequence_schema,sequence_name ";
    private static final String LQ_SCHEMA = " sequence_schema ";

    private static final String LS_BASE = "SELECT schema_name \"Name\" ";
    private static final String LS_DETAIL = " ";
    private static final String LS_FROM = " FROM information_schema.schemata ";
    private static final String LS_WHERE = " WHERE schema_name LIKE ? ";
    private static final String LS_ORDER = " ORDER BY schema_name ";
    private static final String LS_SCHEMA = " schema_name ";

    private static final String LT_BASE = "SELECT table_schema \"Schema\", table_name \"Name\" ";
    private static final String LT_DETAIL = ", table_type \"Type\", table_id \"ID\", storage_name \"Storage Name\" ";
    private static final String LT_FROM = " FROM information_schema.tables ";
    private static final String LT_WHERE = " WHERE table_schema LIKE ? AND table_name LIKE ? AND table_type LIKE '%TABLE'";
    private static final String LT_ORDER = " ORDER BY table_schema,table_name ";
    private static final String LT_SCHEMA = " table_schema ";

    private static final String LV_BASE = "SELECT table_schema \"Schema\", table_name \"Name\" ";
    private static final String LV_DETAIL = ", table_type \"Type\" ";
    private static final String LV_FROM = " FROM information_schema.tables ";
    private static final String LV_WHERE = " WHERE table_schema LIKE ? AND table_name LIKE ? AND table_type LIKE '%VIEW'";
    private static final String LV_ORDER = " ORDER BY table_schema,table_name ";
    private static final String LV_SCHEMA = " table_schema ";

    private static final String L_BASE = "SELECT * ";
    private static final String L_DETAIL = " ";
    private static final String L_FROM = " FROM (" + LT_BASE + ", table_type \"Type\" " + LT_FROM + " UNION "+
                                                     LV_BASE + ", table_type \"Type\" " + LV_FROM + " UNION "+
                                                     LQ_BASE +", 'SEQUENCE' " + LQ_FROM + ") sub ";
    private static final String L_WHERE = "WHERE \"Schema\" LIKE ? AND \"Name\" LIKE ? ";
    private static final String L_ORDER = " ORDER BY \"Schema\", \"Name\", \"Type\" ";
    private static final String L_SCHEMA = "\"Schema\"";


    public static String describeSequence(boolean isSystem, boolean isDetail) {
        return build(DQ_BASE, DQ_DETAIL, DQ_FROM, DQ_WHERE, DQ_ORDER, DQ_SCHEMA, isSystem, isDetail);
    }

    public static String describeTable(boolean isSystem, boolean isDetail) {
        return build(DTV_BASE, DTV_DETAIL, DTV_FROM, DTV_WHERE, DTV_ORDER, DTV_SCHEMA, isSystem, isDetail);
    }

    public static String describeTable_Indexes() {
        return build(DT_INDEXES_BASE, "", DT_INDEXEES_FROM, DT_INDEXES_WHERE, DT_INDEXES_ORDER, "", true, false);
    }

    public static String describeTable_FKReferences() {
        return build(DT_FK_REFERENCES_BASE, "", DT_FK_REFERENCES_FROM, DT_FK_REFERENCES_WHERE, DT_FK_REFERENCES_ORDER, "", true, false);
    }

    public static String describeTable_GFKReferences() {
        return build(DT_GFK_REFERENCES_BASE, "", DT_GFK_REFERENCES_FROM, DT_GFK_REFERENCES_WHERE, DT_GFK_REFERENCES_ORDER, "", true, false);
    }

    public static String describeTable_FKReferencedBy() {
        return build(DT_FK_REFERENCED_BY_BASE, "", DT_FK_REFERENCED_BY_FROM, DT_FK_REFERENCED_BY_WHERE, DT_FK_REFERENCED_BY_ORDER, "", true, false);
    }

    public static String describeTable_GFKReferencedBy() {
        return build(DT_GFK_REFERENCED_BY_BASE, "", DT_GFK_REFERENCED_BY_FROM, DT_GFK_REFERENCED_BY_WHERE, DT_GFK_REFERENCED_BY_ORDER, "", true, false);
    }

    public static String describeView(boolean isSystem, boolean isDetail) {
        return describeTable(isSystem, isDetail);
    }

    public static String listAll(boolean inSystem) {
        return build(L_BASE, L_DETAIL, L_FROM, L_WHERE, L_ORDER, L_SCHEMA, inSystem, false);
    }

    public static String listIndexes(boolean isSystem, boolean isDetail) {
        return build(LI_BASE, LI_DETAIL, LI_FROM, LI_WHERE, LI_ORDER, LI_SCHEMA, isSystem, isDetail);
    }
    
    public static String listSequences(boolean isSystem, boolean isDetail) {
        return build(LQ_BASE, LQ_DETAIL, LQ_FROM, LQ_WHERE, LQ_ORDER, LQ_SCHEMA, isSystem, isDetail);
    }
    
    public static String listSchemata(boolean isSystem, boolean isDetail) {
        return build(LS_BASE, LS_DETAIL, LS_FROM, LS_WHERE, LS_ORDER, LS_SCHEMA, isSystem, isDetail);
    }
    
    public static String listTables(boolean isSystem, boolean isDetail) {
        return build(LT_BASE, LT_DETAIL, LT_FROM, LT_WHERE, LT_ORDER, LT_SCHEMA, isSystem, isDetail);
    }

    public static String listViews(boolean isSystem, boolean isDetail) {
        return build(LV_BASE, LV_DETAIL, LV_FROM, LV_WHERE, LV_ORDER, LV_SCHEMA, isSystem, isDetail);
    }

    public static String build(String base, String detail, String from, String where, String order, String schema,
                               boolean isSystem, boolean isDetail) {
        return base + (isDetail ? detail : "") + from + where + (isSystem ? "" : " AND " + schema + NOT_IN_SYSTEM) + order;
    }
}