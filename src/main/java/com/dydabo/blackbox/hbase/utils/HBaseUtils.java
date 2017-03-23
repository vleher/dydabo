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
package com.dydabo.blackbox.hbase.utils;

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
     */
    public boolean createTable(T row, Connection connection) throws IOException {
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
     */
    public HBaseTable convertRowToHTable(T row, boolean includeObject) throws JsonSyntaxException {
        Map<String, Object> thisValueMap = new Gson().fromJson(row.getBBJson(), Map.class);
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
     * @param row    the value of row
     * @param hTable the value of hTable
     *
     * @return the boolean
     *
     * @throws java.io.IOException
     */
    public boolean checkIfRowExists(T row, Table hTable) throws IOException {
        Get get = new Get(Bytes.toBytes(row.getBBRowKey()));
        Result result = hTable.get(get);
        if (result.isEmpty()) {
            return false;
        } else {
            return true;
        }
    }

}
