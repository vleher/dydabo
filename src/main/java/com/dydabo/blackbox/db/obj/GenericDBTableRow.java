/*
 * Copyright 2017 viswadas leher .
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */
package com.dydabo.blackbox.db.obj;

import com.dydabo.blackbox.common.DyDaBoUtils;
import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonPrimitive;
import java.text.DecimalFormat;
import java.util.HashMap;
import java.util.Map;

/**
 * @author viswadas leher
 */
public class GenericDBTableRow {

    // NOTE: The ColumnFamily names should be as small as possible for performance
    /**
     *
     */
    private static final String DEFAULT_FAMILY = "D";

    private String rowKey;
    private Map<String, ColumnFamily> columnFamilies = null;

    /**
     * @param rowKey
     */
    public GenericDBTableRow(String rowKey) {
        this.rowKey = rowKey;
        this.columnFamilies = new HashMap<>();
        createFamily(DEFAULT_FAMILY);
    }

    /**
     * @param familyName
     *
     * @return
     */
    public ColumnFamily getColumnFamily(String familyName) {
        ColumnFamily colFamily = getColumnFamilies().get(familyName);
        if (colFamily == null) {
            colFamily = createFamily(familyName);
        }
        return colFamily;
    }

    /**
     * @return
     */
    public ColumnFamily getDefaultFamily() {
        return createFamily(DEFAULT_FAMILY);
    }

    /**
     * @param familyName
     *
     * @return
     */
    public ColumnFamily createFamily(String familyName) {
        getColumnFamilies().computeIfAbsent(familyName, ColumnFamily::new);
        return getColumnFamilies().get(familyName);
    }

    /**
     * @return
     */
    public String getRowKey() {
        return rowKey;
    }

    /**
     * @param rowKey
     */
    public void setRowKey(String rowKey) {
        this.rowKey = rowKey;
    }

    /**
     * @return
     */
    public Map<String, ColumnFamily> getColumnFamilies() {
        return columnFamilies;
    }

    /**
     * @return
     */
    public JsonObject toJsonObject() {
        JsonObject jsonObject = getDefaultFamily().toJsonObject();

        for (ColumnFamily columnFamily : getColumnFamilies().values()) {
            if (!DEFAULT_FAMILY.equals(columnFamily.getFamilyName())) {
                jsonObject.add(columnFamily.getFamilyName(), columnFamily.toJsonObject());
            }
        }

        return jsonObject;
    }

    public void forEach(DBTableIterator dbTableIterator) {
        getColumnFamilies().forEach((familyName, columnFamily) -> columnFamily.getColumns().forEach((columnName, column) -> {
            dbTableIterator.accept(familyName, columnName, column.getColumnValue(), column.getColumnValueAsString());
        }));

    }

    @Override
    public String toString() {
        return "HBaseTable{" + "rowKey=" + rowKey + ", columnFamilies=" + columnFamilies + '}';
    }

    /**
     * Collection of columns. This is the child of table rows and contains atleast one column family.
     */
    public class ColumnFamily {

        private String familyName = null;
        private Map<String, Column> columns = null;

        /**
         * @param familyName
         */
        public ColumnFamily(String familyName) {
            this.familyName = familyName;
            this.columns = new HashMap<>();
        }

        /**
         * @param columnName
         * @param columnValue
         */
        public void addColumn(String columnName, Object columnValue) {
            getColumns().put(columnName, new Column(columnName, columnValue));
//            if (columnValue instanceof Map) {
//                Map<Object, Object> thisMap = (Map) columnValue;
//                for (Map.Entry<Object, Object> entry : thisMap.entrySet()) {
//                    String key = String.valueOf(entry.getKey());
//                    Object value = entry.getValue();
//                    if (value != null) {
//                        getColumns().put(key, new Column(key, value));
//                    }
//                }
//            } else {
//            getColumns().put(columnName, new Column(columnName, columnValue));
//            }
        }

        /**
         * @param colName
         *
         * @return
         */
        public Column getColumn(String colName) {
            return getColumns().get(colName);
        }

        /**
         * @return
         */
        public String getFamilyName() {
            return familyName;
        }

        /**
         * @param familyName
         */
        public void setFamilyName(String familyName) {
            this.familyName = familyName;
        }

        /**
         * @return
         */
        public Map<String, Column> getColumns() {
            return columns;
        }

        /**
         * @param columns
         */
        public void setColumns(Map<String, Column> columns) {
            this.columns = columns;
        }

        /**
         * @return
         */
        public JsonObject toJsonObject() {
            JsonObject jsonObject = new JsonObject();
            for (Column coln : getColumns().values()) {
                String keyName = coln.getColumnName();
                String value = coln.getColumnValueAsString();
                if (!DyDaBoUtils.isBlankOrNull(value)) {
                    JsonElement elem = DyDaBoUtils.parseJsonString(value);
                    if (elem != null) {
                        jsonObject.add(keyName, elem);
                    } else {
                        jsonObject.add(keyName, new JsonPrimitive(value));
                    }
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
     * Columns inside a column family
     */
    public class Column {

        private String columnName = null;
        private Object columnValue = null;

        /**
         * @param columnName
         * @param columnValue
         */
        public Column(String columnName, Object columnValue) {
            this.columnName = columnName;
            this.columnValue = columnValue;
        }

        /**
         * @return
         */
        public String getColumnName() {
            return columnName;
        }

        /**
         * @param columnName
         */
        public void setColumnName(String columnName) {
            this.columnName = columnName;
        }

        /**
         * @return
         */
        public Object getColumnValue() {
            return columnValue;
        }

        /**
         * @param columnValue
         */
        public void setColumnValue(Object columnValue) {
            this.columnValue = columnValue;
        }

        /**
         * @return
         */
        public String getColumnValueAsString() {
            if (columnValue == null) {
                return null;
            }
            if (DyDaBoUtils.isPrimitiveOrPrimitiveWrapperOrString(columnValue)) {
                if (columnValue instanceof Number) {
                    DecimalFormat df = new DecimalFormat("#");
                    df.setMaximumFractionDigits(18);
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
            return "Column{" + "columnName=" + columnName + ", columnValue=" + new Gson().toJson(columnValue) + '}';
        }

    }
}
