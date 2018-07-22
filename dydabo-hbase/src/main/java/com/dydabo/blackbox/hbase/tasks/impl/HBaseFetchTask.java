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
package com.dydabo.blackbox.hbase.tasks.impl;

import com.dydabo.blackbox.BlackBoxException;
import com.dydabo.blackbox.BlackBoxable;
import com.dydabo.blackbox.common.utils.DyDaBoUtils;
import com.dydabo.blackbox.db.obj.GenericDBTableRow;
import com.dydabo.blackbox.hbase.tasks.HBaseTask;
import com.google.gson.Gson;
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

import java.io.IOException;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * @param <T>
 * @author viswadas leher
 */
public class HBaseFetchTask<T extends BlackBoxable> extends HBaseTask<T> {

    private final Logger logger = Logger.getLogger(HBaseFetchTask.class.getName());
    private final long maxResults;
    private final List<String> rowKeys;
    private final boolean isPartialKeys;
    private T bean = null;

    /**
     * @param connection
     * @param rowKey
     * @param row
     * @param isPartialKeys
     */
    public HBaseFetchTask(Connection connection, String rowKey, T row, boolean isPartialKeys) {
        this(connection, Collections.singletonList(rowKey), row, isPartialKeys);
    }

    /**
     * @param connection
     * @param rowKeys
     * @param row
     * @param isPartialKeys
     */
    public HBaseFetchTask(Connection connection, List<String> rowKeys, T row, boolean isPartialKeys) {
        this(connection, rowKeys, row, isPartialKeys, -1);
    }

    /**
     * @param connection
     * @param rowKeys
     * @param row
     * @param isPartialKeys
     * @param maxResults
     */
    public HBaseFetchTask(Connection connection, List<String> rowKeys, T row, boolean isPartialKeys, long maxResults) {
        super(connection);
        this.rowKeys = rowKeys;
        this.bean = row;
        this.isPartialKeys = isPartialKeys;
        this.maxResults = maxResults;
    }

    /**
     * @param rowKeys
     * @return
     * @throws BlackBoxException
     */
    private List<T> fetch(List<String> rowKeys) {
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
                        GenericDBTableRow resultTable = utils.parseResultToHTable(result, bean);

                        T resultObject = new Gson().fromJson(resultTable.toJsonObject(), (Type) bean.getClass());
                        if (resultObject != null) {
                            allResults.add(resultObject);
                        }
                    }
                }
            }
        } catch (IOException ex) {
            logger.log(Level.SEVERE, null, ex);
        }

        return allResults;
    }

    private List<T> fetchByPartialKeys(List<String> rowKeys) {
        List<T> results = new ArrayList<>();
        try (Admin admin = getConnection().getAdmin()) {
            try (Table hTable = admin.getConnection().getTable(utils.getTableName(bean))) {
                for (String rowKey : rowKeys) {
                    Scan scan = new Scan();

                    if (maxResults > 0) {
                        scan.setMaxResultSize(maxResults);
                    }

                    String rowPrefix = DyDaBoUtils.getStringPrefix(rowKey);

                    Filter rowFilter = new RowFilter(CompareFilter.CompareOp.EQUAL, new RegexStringComparator(rowKey));
                    if (!DyDaBoUtils.isBlankOrNull(rowPrefix)) {
                        scan.setRowPrefixFilter(Bytes.toBytes(rowPrefix));
                    }
                    logger.finest(rowFilter.toString());
                    scan.setFilter(rowFilter);

                    try (ResultScanner resultScanner = hTable.getScanner(scan)) {
                        int count = 0;
                        for (Result result : resultScanner) {
                            GenericDBTableRow resultTable = utils.parseResultToHTable(result, bean);

                            T resultObject = new Gson().fromJson(resultTable.toJsonObject(), (Type) bean.getClass());
                            if (resultObject != null) {
                                results.add(resultObject);
                                count++;
                            }

                            if (maxResults > 0 && count >= maxResults) {
                                break;
                            }
                        }
                    }
                }

            }
        } catch (IOException ex) {
            logger.log(Level.SEVERE, null, ex);
        }
        return results;
    }

    @Override
    protected List<T> compute() {
            return fetch(rowKeys);

    }

}
