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
import java.io.OutputStream;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.List;
import java.util.ArrayList;

public class ResultPrinter
{
    private static enum ALIGN { LEFT, CENTER, RIGHT, OFF }

    private OutputSink sink;
    private int columnCount;
    private int[] cellWidths;
    private boolean[] isNumber;
    private boolean expandedOutput = false;
    private String nullString = "";
    private boolean tupleOutput = false;
    private String fieldSeparator = "|";
    private boolean aligned = true;

    public ResultPrinter(OutputSink sink) {
        this.sink = sink;
    }

    public void setSink(OutputSink sink) {
        assert sink != null;
        this.sink = sink;
    }

    public void changeExpandedOutput(){
        expandedOutput = !expandedOutput;
    }

    public void changeExpandedOutput(boolean truth){
        expandedOutput = truth;
    }

    public boolean getExpandedOutput() {
        return expandedOutput;
    }

    public void changeNullOutput(String output) {
        nullString = output;
    }

    public void changeNullOutput(){
        nullString = "";
    }

    public void changeTupleOutput(){
        tupleOutput = !tupleOutput;
    }

    public void changeTupleOutput(boolean truth){
        tupleOutput = truth;
    }

    public boolean getTupleOutput() {
        return tupleOutput;
    }

    public String getFieldSeparator(){
        return fieldSeparator;
    }

    public void setFieldSeparator(String fieldSeparator){
        this.fieldSeparator = fieldSeparator;
    }

    public void changeAlignment(){
        aligned = !aligned;
    }

    public void changeAlignment(boolean truth){
         aligned = truth;
    }

    public boolean getAlignment(){
        return aligned;
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
        int rowCount = 0;
        if (expandedOutput) {
            List<String> columnNames = new ArrayList<>(1);
            int maxLength = 0;
            for (int i = 0; i < md.getColumnCount(); ++i) {
                String s = md.getColumnLabel(i + 1);
                if(s.length() > maxLength)
                    maxLength = s.length();
                columnNames.add(s);

            }
            rs.beforeFirst();
            while (rs.next()) {
                if(!tupleOutput) {
                    sink.print("-[ RECORD " + (rowCount + 1) + " ]\n");
                }
                else {
                    if(getFieldSeparator() == "|")
                        sink.print("-\n");
                    else
                        sink.print(getFieldSeparator() + "\n");
                }
                for (int i = 0; i < columnCount; ++i) {
                    String s = rs.getString(i + 1);
                    if (s == null) {
                        s = "";
                    }
                    if(!tupleOutput) {
                        String c = columnNames.get(i);
                        sink.print(c);
                        if(aligned) {
                            spaceFill(sink, maxLength - c.length());
                            if(s == "")
                                sink.print(" " + getFieldSeparator() + "\n");
                            else
                                sink.print(" " + getFieldSeparator() + " " + s + "\n");
                        } else {
                            sink.print(getFieldSeparator() + s + "\n");
                        }
                    } else {
                        sink.print(s + "\n");
                    }
                }
                ++rowCount;
            }
        } else {
            if(!tupleOutput) {
                for (int i = 0; i < md.getColumnCount(); ++i) {
                    metaColumn(i, md.isSigned(i + 1), md.getColumnLabel(i + 1), (i == columnCount - 1));
                }
                if(aligned) {
                    metaFinish();
                }
            }
            rs.beforeFirst();
            while (rs.next()) {
                for (int i = 0; i < columnCount; ++i) {
                    String s = rs.getString(i + 1);
                    rowsColumn(i, (s == null) ? getNullString() : s, (i == columnCount - 1));
                }
                rowsRowFinish();
                ++rowCount;
            }
            rowsFinish(rowCount);
        }
        sink.println();
    }

    public void printUpdate(int updateCount) throws IOException {
        sink.print("ROWS: ");
        sink.println(Integer.toString(updateCount));
        sink.println();
    }

    public void printError(String msg) throws IOException {
        sink.printlnError(msg);
    }

    public void printError(SQLException ex) throws IOException {
        // Message from server already includes 'ERROR: '
        appendException(ex, "");
        sink.printlnError();
    }

    public void printWarning(SQLException ex) throws IOException {
        appendException(ex, "WARNING: ");
    }

    //
    // Internal
    //

    public String getNullString() {
        return nullString;
    }

    private void metaColumn(int column, boolean isNumeric, String label, boolean finalColumn) throws IOException {
        if(!aligned){
            appendCell(sink, column == 0, cellWidths[column], ALIGN.OFF, label, getFieldSeparator(), false);
        } else {
            appendCell(sink, column == 0, cellWidths[column], ALIGN.CENTER, label, getFieldSeparator(), finalColumn);
        }
    }

    private void metaFinish() throws IOException {
        sink.println();
        for(int i = 0; i < columnCount; ++i) {
            if(i > 0) {
                sink.print('+');
            }
            for(int j = 0; j < (cellWidths[i] + 1) + getFieldSeparator().length(); ++j) {
                sink.print('-');
            }
        }
        sink.println();
    }

    private void rowsColumn(int column, String value, boolean finalColumn) throws IOException {
        if(!aligned){
            appendCell(sink, column == 0, cellWidths[column], ALIGN.OFF, value, getFieldSeparator(), false);
        } else {
            appendCell(sink, column == 0, cellWidths[column], isNumber[column] ? ALIGN.RIGHT : ALIGN.LEFT, value, getFieldSeparator(), finalColumn);
        }
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
        sink.printError(prefix);
        sink.printlnError(msg);
    }

    private static void appendCell(OutputSink sink, boolean isFirst, int width, ALIGN align, String value, String fieldSeparator, boolean finalColumn) throws IOException {
        if(!isFirst) {
            sink.print(fieldSeparator);
        }

        int alignDiff = width - value.length();
        switch(align) {
            case LEFT:
                if(!finalColumn || value != "") {
                    sink.print(' ');
                    sink.print(value);
                }
                if(!finalColumn) {
                    spaceFill(sink, alignDiff);
                    sink.print(' ');
                }
            break;
            case CENTER:
                int halfDiff = alignDiff / 2;
                int slop = alignDiff & 1;
                if(!finalColumn || value != "") {
                    sink.print(' ');
                    spaceFill(sink, halfDiff);
                    sink.print(value);
                }
                if(!finalColumn) {
                    spaceFill(sink, halfDiff + slop);
                    sink.print(' ');
                }
            break;
            case RIGHT:
                if(!finalColumn || value != "") {
                    sink.print(' ');
                    spaceFill(sink, alignDiff);
                    sink.print(value);
                }
                if(!finalColumn) {
                    sink.print(' ');
                }
            break;
            case OFF:
                sink.print(value);
            break;
            default:
                assert false;
        }
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