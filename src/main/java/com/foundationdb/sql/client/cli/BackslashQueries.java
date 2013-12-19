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

    private static final String DTV_BASE = "SELECT column_name \"Column\", CONCAT(data_type, COALESCE(CONCAT('(', character_maximum_length, ')'), "+
                                           " CONCAT('(', numeric_precision, ',', numeric_scale, ')'), '')) \"Type\", is_nullable \"Nullable\" ";
    private static final String DTV_DETAIL = ", column_default \"Default\",  character_set_name \"Charset\", collation_name \"Collation\", CONCAT(sequence_schema, '.', sequence_name) \"Sequence\" ";
    private static final String DTV_FROM = " FROM information_schema.columns ";
    private static final String DTV_WHERE = " WHERE table_schema LIKE ? AND table_name LIKE ? ";
    private static final String DTV_ORDER = " ORDER BY ordinal_position ";
    private static final String DTV_SCHEMA = " table_schema ";

    private static final String LI_BASE = "SELECT table_schema \"Schema\", table_name \"Table\", index_name \"Name\" ";
    private static final String LI_DETAIL = ", is_unique \"Unique\", join_type \"Join Type\", index_method \"Index Method\", storage_name \"Storage Name\" ";
    private static final String LI_FROM = " FROM information_schema.indexes ";
    private static final String LI_WHERE = " WHERE table_schema LIKE ? AND table_name LIKE ? AND index_name LIKE ? ";
    private static final String LI_ORDER = " ORDER BY table_schema,table_name,index_name ";
    private static final String LI_SCHEMA = " table_schema ";

    private static final String LQ_BASE = "SELECT sequence_schema \"Schema\", sequence_name \"Name\" ";
    private static final String LQ_DETAIL = ", start_value \"Start\", minimum_value \"Min\", maximum_value \"Max\", increment \"Inc\", cycle_option \"Cycle\", storage_name \"Storage Name\" ";
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


    public static String describeTableOrView(boolean isSystem, boolean isDetail) {
        return build(DTV_BASE, DTV_DETAIL, DTV_FROM, DTV_WHERE, DTV_ORDER, DTV_SCHEMA, isSystem, isDetail);
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