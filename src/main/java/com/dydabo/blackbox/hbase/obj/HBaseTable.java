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
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonPrimitive;
import java.text.DecimalFormat;
import java.util.HashMap;
import java.util.Map;

/**
 *
 * @author viswadas leher <vleher@gmail.com>
 */
public class HBaseTable {

    // The ColumnFamily names should be as small as possible for performance
    /**
     *
     */
    public static final String DEFAULT_FAMILY = "D";

    private String rowKey;
    private Map<String, ColumnFamily> columnFamilies = null;

    /**
     *
     * @param rowKey
     */
    public HBaseTable(String rowKey) {
        this.rowKey = rowKey;
        this.columnFamilies = new HashMap<>();
        createFamily(DEFAULT_FAMILY);
    }

    public ColumnFamily getColumnFamily(String familyName) {
        ColumnFamily colFamily = getColumnFamilies().get(familyName);
        if (colFamily == null) {
            colFamily = createFamily(familyName);
        }
        return colFamily;
    }

    /**
     *
     * @return
     */
    public ColumnFamily getDefaultFamily() {
        return createFamily(DEFAULT_FAMILY);
    }

    /**
     *
     * @param familyName
     *
     * @return
     */
    public ColumnFamily createFamily(String familyName) {
        ColumnFamily thisFamily = getColumnFamilies().get(familyName);
        if (thisFamily == null) {
            thisFamily = getColumnFamilies().put(familyName, new ColumnFamily(familyName));
        }
        return getColumnFamilies().get(familyName);
    }

    /**
     *
     * @return
     */
    public String getRowKey() {
        return rowKey;
    }

    /**
     *
     * @param rowKey
     */
    public void setRowKey(String rowKey) {
        this.rowKey = rowKey;
    }

    /**
     *
     * @return
     */
    public Map<String, ColumnFamily> getColumnFamilies() {
        return columnFamilies;
    }

    public JsonObject toJsonObject() {
        JsonObject jsonObject = getDefaultFamily().toJsonObject();

        for (ColumnFamily columnFamily : getColumnFamilies().values()) {
            if (!DEFAULT_FAMILY.equals(columnFamily.getFamilyName())) {
                jsonObject.add(columnFamily.getFamilyName(), columnFamily.toJsonObject());
            }
        }

        return jsonObject;
    }

    @Override
    public String toString() {
        return "HBaseTable{" + "rowKey=" + rowKey + ", columnFamilies=" + columnFamilies + '}';
    }

    /**
     *
     */
    public class ColumnFamily {

        private String familyName = null;
        private Map<String, Column> columns = null;

        /**
         *
         * @param familyName
         */
        public ColumnFamily(String familyName) {
            this.familyName = familyName;
            this.columns = new HashMap<>();
        }

        /**
         *
         * @param columnName
         * @param columnValue
         */
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

        /**
         *
         * @return
         */
        public String getFamilyName() {
            return familyName;
        }

        /**
         *
         * @param familyName
         */
        public void setFamilyName(String familyName) {
            this.familyName = familyName;
        }

        /**
         *
         * @return
         */
        public Map<String, Column> getColumns() {
            return columns;
        }

        /**
         *
         * @param columns
         */
        public void setColumns(Map<String, Column> columns) {
            this.columns = columns;
        }

        public JsonObject toJsonObject() {
            JsonObject jsonObject = new JsonObject();
            for (Column coln : getColumns().values()) {
                String keyName = coln.getColumnName();
                String value = coln.getColumnValueAsString();
                JsonElement elem = DyDaBoUtils.parseJsonString(value);
                if (elem != null) {
                    jsonObject.add(keyName, elem);
                } else {
                    jsonObject.add(keyName, new JsonPrimitive(value));
                }
            }
            return jsonObject;
        }

        @Override
        public String toString() {
            return "ColumnFamily{" + "familyName=" + familyName + ", column=" + columns + '}';
        }

    }

    /**
     *
     */
    public class Column {

        private String columnName = null;
        private Object columnValue = null;

        /**
         *
         * @param columnName
         * @param columnValue
         */
        public Column(String columnName, Object columnValue) {
            this.columnName = columnName;
            this.columnValue = columnValue;
        }

        /**
         *
         * @return
         */
        public String getColumnName() {
            return columnName;
        }

        /**
         *
         * @param columnName
         */
        public void setColumnName(String columnName) {
            this.columnName = columnName;
        }

        public Object getColumnValue() {
            return columnValue;
        }

        /**
         *
         * @param columnValue
         */
        public void setColumnValue(Object columnValue) {
            this.columnValue = columnValue;
        }

        /**
         *
         * @return
         */
        public String getColumnValueAsString() {
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

        @Override
        public String toString() {
            return "Column{" + "columnName=" + columnName + ", columnValue=" + columnValue + '}';
        }

    }
}
