/*******************************************************************************
 * Copyright 2017 viswadas leher <vleher@gmail.com>.
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
 *******************************************************************************/
package com.dydabo.blackbox.hbase.utils;

import com.dydabo.blackbox.BlackBoxException;
import com.dydabo.blackbox.BlackBoxable;
import com.dydabo.blackbox.common.DyDaBoUtils;
import com.dydabo.blackbox.hbase.obj.HBaseTable;
import com.google.gson.Gson;
import com.google.gson.JsonSyntaxException;
import java.io.IOException;
import java.util.Map;
import java.util.SortedSet;
import java.util.TreeSet;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

/**
 *
 * @author viswadas leher <vleher@gmail.com>
 * @param <T>
 */
public class HBaseUtils<T extends BlackBoxable> {

    private static SortedSet tableCache = new TreeSet();

    /**
     *
     * @param row
     *
     * @return
     */
    public TableName getTableName(T row) {
        final String fullClassName = row.getClass().toString().substring(6).replaceAll("\\.", "");
        return TableName.valueOf(fullClassName);
    }

    /**
     *
     * @param row        the value of row
     * @param connection
     *
     * @return the boolean
     *
     * @throws java.io.IOException
     * @throws com.dydabo.blackbox.BlackBoxException
     */
    public boolean createTable(T row, Connection connection) throws IOException, BlackBoxException {
        try (Admin admin = connection.getAdmin()) {
            Object lockObject = new Object();
            TableName tableName = getTableName(row);

            if (tableCache.contains(tableName)) {
                return true;
            }

            if (admin.tableExists(tableName)) {
                tableCache.add(tableName);
                return true;
            }

            synchronized (lockObject) {
                HTableDescriptor tableDescriptor = new HTableDescriptor(tableName);

                HBaseTable hTable = convertRowToHTable(row, true);
                for (HBaseTable.ColumnFamily value : hTable.getColumnFamilies().values()) {
                    HColumnDescriptor dFamily = new HColumnDescriptor(value.getFamilyName());
                    tableDescriptor.addFamily(dFamily);
                }
                admin.createTable(tableDescriptor);
                tableCache.add(tableName);
            }

            return true;
        }
    }

    /**
     *
     * @param row
     * @param includeObject
     *
     * @return
     *
     * @throws JsonSyntaxException
     * @throws com.dydabo.blackbox.BlackBoxException
     */
    public HBaseTable convertRowToHTable(T row, boolean includeObject) throws JsonSyntaxException, BlackBoxException {
        // TODO: this can be better
        String rowJson = new Gson().toJson(row);
        Map<String, Object> thisValueMap = new Gson().fromJson(rowJson, Map.class);

        HBaseTable hbaseTable = new HBaseTable(row.getBBRowKey());
        for (Map.Entry<String, Object> entry : thisValueMap.entrySet()) {
            String key = entry.getKey();
            Object value = entry.getValue();

            if (DyDaBoUtils.isPrimitiveOrPrimitiveWrapperOrString(value)) {
                hbaseTable.getDefaultFamily().addColumn(key, value);
            } else if (includeObject) {
                if (value instanceof Map) {
                    hbaseTable.createFamily(key).addColumn(key, value);
                } else {
                    hbaseTable.getDefaultFamily().addColumn(key, value);
                }

            }
        }

        return hbaseTable;
    }

    /**
     *
     * Checks if the specified row already exists in the table
     *
     * @param row    the value of row
     * @param hTable the value of hTable
     *
     * @return the boolean
     *
     * @throws java.io.IOException
     * @throws com.dydabo.blackbox.BlackBoxException
     */
    public boolean checkIfRowExists(T row, Table hTable) throws IOException, BlackBoxException {
        if (!isValidRowKey(row)) {
            throw new BlackBoxException("Invalid Row Key");
        }
        Get get = new Get(Bytes.toBytes(row.getBBRowKey()));
        Result result = hTable.get(get);
        if (result.isEmpty()) {
            return false;
        } else {
            return true;
        }
    }

    /**
     *
     * @param regexValue
     *
     * @return
     */
    public String sanitizeRegex(String regexValue) {
        // TODO: alter regex to search inside lists and arrays
        return regexValue;
    }

    /**
     *
     * @param row
     *
     * @return
     */
    public boolean isValidRowKey(T row) {
        if (row == null) {
            return false;
        }
        if (DyDaBoUtils.isBlankOrNull(row.getBBRowKey())) {
            return false;
        }
        return true;
    }

}
