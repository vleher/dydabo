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
import org.apache.hadoop.hbase.client.*;
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
    private final List<T> rowKeys;
    private final boolean isPartialKeys;

    /**
     * @param connection
     * @param rowKey
     * @param isPartialKeys
     */
    public HBaseFetchTask(Connection connection, T rowKey, boolean isPartialKeys) {
        this(connection, Collections.singletonList(rowKey), isPartialKeys);
    }

    /**
     * @param connection
     * @param rowKeys
     * @param isPartialKeys
     */
    public HBaseFetchTask(Connection connection, List<T> rowKeys, boolean isPartialKeys) {
        this(connection, rowKeys, isPartialKeys, -1);
    }

    /**
     * @param connection
     * @param rowKeys
     * @param isPartialKeys
     * @param maxResults
     */
    public HBaseFetchTask(Connection connection, List<T> rowKeys, boolean isPartialKeys, long maxResults) {
        super(connection);
        this.rowKeys = rowKeys;
        this.isPartialKeys = isPartialKeys;
        this.maxResults = maxResults;
    }

    /**
     * @param rows
     * @return
     * @throws BlackBoxException
     */
    private List<T> fetch(List<T> rows) {
        if (isPartialKeys) {
            return fetchByPartialKeys(rows);
        }

        List<T> allResults = new ArrayList<>();

        try (Admin admin = getConnection().getAdmin()) {
            // consider create to be is nothing but alter...so
            try (Table hTable = admin.getConnection().getTable(utils.getTableName(rows.get(0)))) {
                List<Get> getList = new ArrayList<>();

                for (T row : rows) {
                    Get g = new Get(Bytes.toBytes(row.getBBRowKey()));
                    getList.add(g);
                }

                Result[] results = hTable.get(getList);
                for (Result result : results) {
                    if (result.listCells() != null) {
                        GenericDBTableRow resultTable = utils.parseResultToHTable(result, rows.get(0));

                        T resultObject = new Gson().fromJson(resultTable.toJsonObject(), (Type) rows.get(0).getClass());
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

    private List<T> fetchByPartialKeys(List<T> rows) {
        List<T> results = new ArrayList<>();
        try (Admin admin = getConnection().getAdmin()) {
            try (Table hTable = admin.getConnection().getTable(utils.getTableName(rows.get(0)))) {
                for (T row : rows) {
                    Scan scan = new Scan();

                    if (maxResults > 0) {
                        scan.setMaxResultSize(maxResults);
                    }

                    String rowPrefix = DyDaBoUtils.getStringPrefix(row.getBBRowKey());

                    Filter rowFilter = new RowFilter(CompareFilter.CompareOp.EQUAL, new RegexStringComparator(row.getBBRowKey()));
                    if (!DyDaBoUtils.isBlankOrNull(rowPrefix)) {
                        scan.setRowPrefixFilter(Bytes.toBytes(rowPrefix));
                    }
                    logger.finest(rowFilter.toString());
                    scan.setFilter(rowFilter);

                    try (ResultScanner resultScanner = hTable.getScanner(scan)) {
                        int count = 0;
                        for (Result result : resultScanner) {
                            GenericDBTableRow resultTable = utils.parseResultToHTable(result, rows.get(0));

                            T resultObject = new Gson().fromJson(resultTable.toJsonObject(), (Type) rows.get(0).getClass());
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
