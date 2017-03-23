/*
 * Copyright (C) 2017 viswadas leher <vleher@gmail.com>
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package com.dydabo.blackbox.hbase.obj;

import com.dydabo.blackbox.common.DyDaBoUtils;
import com.google.gson.Gson;
import java.text.DecimalFormat;
import java.util.HashMap;
import java.util.Map;

/**
 *
 * @author viswadas leher <vleher@gmail.com>
 */
public class HBaseTable {

    // The ColumnFamily names should be as small as possible for performance
    public static final String DEFAULT_FAMILY = "D";

    private String rowKey;
    private Map<String, ColumnFamily> columnFamilies = null;

    public HBaseTable(String rowKey) {
        this.rowKey = rowKey;
        this.columnFamilies = new HashMap<>();
    }

    public ColumnFamily getDefaultFamily() {
        return createFamily(DEFAULT_FAMILY);
    }

    public ColumnFamily createFamily(String familyName) {
        ColumnFamily thisFamily = getColumnFamilies().get(familyName);
        if (thisFamily == null) {
            thisFamily = getColumnFamilies().put(familyName, new ColumnFamily(familyName));
        }
        return getColumnFamilies().get(familyName);
    }

    public String getRowKey() {
        return rowKey;
    }

    public void setRowKey(String rowKey) {
        this.rowKey = rowKey;
    }

    public Map<String, ColumnFamily> getColumnFamilies() {
        return columnFamilies;
    }

    @Override
    public String toString() {
        return "HBaseTable{" + "rowKey=" + rowKey + ", columnFamilies=" + columnFamilies + '}';
    }

    public class ColumnFamily {

        private String familyName = null;
        private Map<String, Column> columns = null;

        public ColumnFamily(String familyName) {
            this.familyName = familyName;
            this.columns = new HashMap<>();
        }

        public void addColumn(String columnName, Object columnValue) {
            if (columnValue instanceof Map) {
                Map<Object, Object> thisMap = (Map) columnValue;
                for (Map.Entry<Object, Object> entry : thisMap.entrySet()) {
                    String key = String.valueOf(entry.getKey());
                    Object value = entry.getValue();
                    if (value != null) {
                        getColumns().put(key, new Column(key, value));
                    }
                }
            } else {
                getColumns().put(columnName, new Column(columnName, columnValue));
            }
        }

        public String getFamilyName() {
            return familyName;
        }

        public void setFamilyName(String familyName) {
            this.familyName = familyName;
        }

        public Map<String, Column> getColumns() {
            return columns;
        }

        public void setColumns(Map<String, Column> columns) {
            this.columns = columns;
        }

        @Override
        public String toString() {
            return "ColumnFamily{" + "familyName=" + familyName + ", column=" + columns + '}';
        }

    }

    public class Column {

        private String columnName = null;
        private Object columnValue = null;

        public Column(String columnName, Object columnValue) {
            this.columnName = columnName;
            this.columnValue = columnValue;
        }

        public String getColumnName() {
            return columnName;
        }

        public void setColumnName(String columnName) {
            this.columnName = columnName;
        }

        public String getColumnValue() {
            if (DyDaBoUtils.isPrimitiveOrPrimitiveWrapperOrString(columnValue)) {
                if (columnValue instanceof Number) {
                    DecimalFormat df = new DecimalFormat("#");
                    df.setMaximumFractionDigits(10);
                    df.setMaximumIntegerDigits(64);
                    return df.format(columnValue);
                } else {
                    return String.valueOf(columnValue);
                }
            } else {
                return (new Gson().toJson(columnValue));
            }
        }

        public void setColumnValue(Object columnValue) {
            this.columnValue = columnValue;
        }

        @Override
        public String toString() {
            return "Column{" + "columnName=" + columnName + ", columnValue=" + columnValue + '}';
        }

    }
}
