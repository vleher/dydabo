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
package com.dydabo.blackbox.hbase.tasks;

import com.dydabo.blackbox.BlackBoxException;
import com.dydabo.blackbox.BlackBoxable;
import com.dydabo.blackbox.hbase.obj.HBaseTable;
import com.dydabo.blackbox.hbase.utils.HBaseUtils;
import com.google.gson.Gson;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.concurrent.RecursiveTask;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.RegexStringComparator;
import org.apache.hadoop.hbase.filter.RowFilter;
import org.apache.hadoop.hbase.util.Bytes;

/**
 *
 * @author viswadas leher <vleher@gmail.com>
 * @param <T>
 */
public class HBaseFetchTask<T extends BlackBoxable> extends RecursiveTask<List<T>> {

    private final Connection connection;
    private final HBaseUtils<T> utils;
    private List<String> rowKeys;
    private T bean = null;
    private boolean isPartialKeys;

    /**
     *
     * @param connection
     * @param rowKey
     * @param row
     */
    public HBaseFetchTask(Connection connection, String rowKey, T row, boolean isPartialKeys) {
        this(connection, Arrays.asList(rowKey), row, isPartialKeys);
    }

    /**
     *
     * @param connection
     * @param rowKeys
     * @param row
     */
    public HBaseFetchTask(Connection connection, List<String> rowKeys, T row, boolean isPartialKeys) {
        this.connection = connection;
        this.rowKeys = rowKeys;
        this.utils = new HBaseUtils<T>();
        this.bean = row;
        this.isPartialKeys = isPartialKeys;
    }

    /**
     *
     * @param rowKeys
     *
     * @return
     *
     * @throws BlackBoxException
     */
    protected List<T> fetch(List<String> rowKeys) throws BlackBoxException {
        if (isPartialKeys) {
            return fetchByPartialKeys(rowKeys);
        }

        List<T> allResults = new ArrayList<>();

        try (Admin admin = getConnection().getAdmin()) {
            // consider create to be is nothing but alter...so
            try (Table hTable = admin.getConnection().getTable(utils.getTableName(bean))) {
                List<Get> getList = new ArrayList<>();

                for (String rowKey : rowKeys) {
                    Get g = new Get(Bytes.toBytes(rowKey));
                    getList.add(g);
                }

                Result[] results = hTable.get(getList);
                for (Result result : results) {
                    if (result.listCells() != null) {
                        HBaseTable resultTable = parseToHBaseTable(result);

                        T resultObject = new Gson().fromJson(resultTable.toJsonObject(), (Class<T>) bean.getClass());
                        if (resultObject != null) {
                            allResults.add(resultObject);
                        }
                    }
                }
            }
        } catch (IOException ex) {
            Logger.getLogger(HBaseFetchTask.class.getName()).log(Level.SEVERE, null, ex);
        }

        return allResults;
    }

    private List<T> fetchByPartialKeys(List<String> rowKeys) {
        List<T> results = new ArrayList<>();
        try (Admin admin = getConnection().getAdmin()) {
            try (Table hTable = admin.getConnection().getTable(utils.getTableName(bean))) {
                for (String rowKey : rowKeys) {
                    Scan scan = new Scan();

                    Filter rowFilter = new RowFilter(CompareFilter.CompareOp.EQUAL, new RegexStringComparator(rowKey));
                    scan.setFilter(rowFilter);

                    try (ResultScanner resultScanner = hTable.getScanner(scan)) {
                        for (Result result : resultScanner) {
                            HBaseTable resultTable = parseToHBaseTable(result);

                            T resultObject = new Gson().fromJson(resultTable.toJsonObject(), (Class<T>) bean.getClass());
                            if (resultObject != null) {
                                results.add(resultObject);
                            }
                        }
                    }
                }

            }
        } catch (IOException ex) {
            Logger.getLogger(HBaseFetchTask.class.getName()).log(Level.SEVERE, null, ex);
        }
        return results;
    }

    @Override
    protected List<T> compute() {
        try {
            return HBaseFetchTask.this.fetch(rowKeys);
        } catch (BlackBoxException ex) {
            Logger.getLogger(HBaseSearchTask.class.getName()).log(Level.SEVERE, null, ex);
        }
        return new ArrayList<>();
    }

    /**
     *
     * @return
     */
    public Connection getConnection() {
        return connection;
    }

    protected HBaseTable parseToHBaseTable(Result result) {
        HBaseTable resultTable = new HBaseTable(Bytes.toString(result.getRow()));
        NavigableMap<byte[], NavigableMap<byte[], byte[]>> map = result.getNoVersionMap();
        for (Map.Entry<byte[], NavigableMap<byte[], byte[]>> entry : map.entrySet()) {
            String familyName = Bytes.toString(entry.getKey());
            NavigableMap<byte[], byte[]> famColMap = entry.getValue();

            for (Map.Entry<byte[], byte[]> cols : famColMap.entrySet()) {
                String colName = Bytes.toString(cols.getKey());
                String colValue = Bytes.toString(cols.getValue());

                resultTable.getColumnFamily(familyName).addColumn(colName, colValue);
            }

        }
        return resultTable;
    }

}
