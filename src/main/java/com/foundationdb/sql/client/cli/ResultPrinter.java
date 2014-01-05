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

import java.io.IOException;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;

public class ResultPrinter
{
    private static enum ALIGN { LEFT, CENTER, RIGHT }

    private final OutputSink sink;
    private int columnCount;
    private int[] cellWidths;
    private boolean[] isNumber;

    public ResultPrinter(OutputSink sink) {
        this.sink = sink;
    }

    public void printResultSet(ResultSet rs) throws SQLException, IOException {
        printResultSet(null, rs);
    }

    public void printResultSet(String description, ResultSet rs) throws SQLException, IOException {
        if(description != null) {
            sink.print(' ');
            sink.println(description);
        }
        ResultSetMetaData md = rs.getMetaData();
        this.columnCount = md.getColumnCount();
        this.cellWidths = computeCellWidths(rs, getNullString());
        this.isNumber = computeIsNumber(rs);

        metaStart();
        for(int i = 0; i < md.getColumnCount(); ++i) {
            metaColumn(i, md.isSigned(i + 1), md.getColumnLabel(i + 1));
        }
        metaFinish();

        rs.beforeFirst();
        int rowCount = 0;
        rowsStart();
        while(rs.next()) {
            rowsRowStart();
            for(int i = 0; i < columnCount; ++i) {
                String s = rs.getString(i + 1);
                rowsColumn(i, (s == null) ? getNullString() : s);
            }
            rowsRowFinish();
            ++rowCount;
        }
        rowsFinish(rowCount);
        sink.println();
    }

    public void printUpdate(int updateCount) throws IOException {
        sink.print("ROWS: ");
        sink.println(Integer.toString(updateCount));
        sink.println();
    }

    public void printError(String msg) throws IOException {
        sink.println(msg);
    }

    public void printError(SQLException ex) throws IOException {
        // Message from server already includes 'ERROR: '
        appendException(ex, "");
        sink.println();
    }

    public void printWarning(SQLException ex) throws IOException {
        appendException(ex, "WARNING: ");
    }

    //
    // Internal
    //

    public String getNullString() {
        return "";
    }

    private void metaStart() {
    }

    private void metaColumn(int column, boolean isNumeric, String label) throws IOException {
        appendCell(sink, column == 0, cellWidths[column], ALIGN.CENTER, label);
    }

    private void metaFinish() throws IOException {
        sink.println();
        for(int i = 0; i < columnCount; ++i) {
            if(i > 0) {
                sink.print('+');
            }
            for(int j = 0; j < (cellWidths[i] + 2); ++j) {
                sink.print('-');
            }
        }
        sink.println();
    }

    private void rowsStart() {
    }

    private void rowsRowStart() {
    }

    private void rowsColumn(int column, String value) throws IOException {
        appendCell(sink, column == 0, cellWidths[column], isNumber[column] ? ALIGN.RIGHT : ALIGN.LEFT, value);
    }

    private void rowsRowFinish() throws IOException {
        sink.println();
    }

    private void rowsFinish(int rowCount) throws IOException {
        sink.print('(');
        sink.print(Integer.toString(rowCount));
        sink.print(" row");
        if(rowCount != 1) {
            sink.print('s');
        }
        sink.print(')');
        sink.println();
    }

    private void appendException(SQLException ex, String prefix) throws IOException {
        // TODO: Option to show code?
        String msg = ex.getMessage().replaceAll("\n  Position.*", "");
        sink.print(prefix);
        sink.println(msg);
    }

    private static void appendCell(OutputSink sink, boolean isFirst, int width, ALIGN align, String value) throws IOException {
        if(!isFirst) {
            sink.print("|");
        }
        sink.print(' ');
        int alignDiff = width - value.length();
        switch(align) {
            case LEFT:
                sink.print(value);
                spaceFill(sink, alignDiff);
            break;
            case CENTER:
                int halfDiff = alignDiff / 2;
                int slop = alignDiff & 1;
                spaceFill(sink, halfDiff);
                sink.print(value);
                spaceFill(sink, halfDiff + slop);
            break;
            case RIGHT:
                spaceFill(sink, alignDiff);
                sink.print(value);
            break;
            default:
                assert false;
        }
        sink.print(' ');
    }

    private static void spaceFill(OutputSink sink, int count) throws IOException {
        for(int i = 0; i < count; ++i) {
            sink.print(' ');
        }
    }

    private static boolean[] computeIsNumber(ResultSet rs) throws SQLException {
        ResultSetMetaData md = rs.getMetaData();
        boolean[] isNumber = new boolean[md.getColumnCount()];
        for(int i = 0; i < md.getColumnCount(); ++i) {
            isNumber[i] = md.isSigned(i + 1);
        }
        return isNumber;
    }

    private static int[] computeCellWidths(ResultSet rs, String nullString) throws SQLException {
        ResultSetMetaData md = rs.getMetaData();
        int[] widths = new int[md.getColumnCount()];
        for(int i = 0; i < md.getColumnCount(); ++i) {
            widths[i] = md.getColumnLabel(i + 1).length();
        }
        rs.beforeFirst();
        while(rs.next()) {
            for(int i = 0; i < widths.length; ++i) {
                String value = rs.getString(i + 1);
                if(value == null) {
                    value = nullString;
                }
                widths[i] = Math.max(widths[i], value.length());
            }
        }
        return widths;
    }
}
