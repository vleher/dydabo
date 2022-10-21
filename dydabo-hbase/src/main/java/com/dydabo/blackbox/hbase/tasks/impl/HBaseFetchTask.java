/*
 *  Copyright 2020 viswadas leher <vleher@gmail.com>.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 */
package com.dydabo.blackbox.hbase.tasks.impl;

import com.dydabo.blackbox.BlackBoxException;
import com.dydabo.blackbox.BlackBoxable;
import com.dydabo.blackbox.common.MaxResultList;
import com.dydabo.blackbox.common.utils.DyDaBoUtils;
import com.dydabo.blackbox.db.obj.GenericDBTableRow;
import com.dydabo.blackbox.hbase.tasks.HBaseTask;
import com.google.gson.Gson;
import org.apache.hadoop.hbase.CompareOperator;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.RegexStringComparator;
import org.apache.hadoop.hbase.filter.RowFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * @param <T>
 * @author viswadas leher
 */
public class HBaseFetchTask<T extends BlackBoxable> extends HBaseTask<T, List<T>> {

    private final Logger logger = LogManager.getLogger();
    private final int maxResults;
    private final List<T> rowKeys;
    private final boolean isPartialKeys;
    private final boolean isFirst;

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
        this(connection, rowKeys, isPartialKeys, Integer.MAX_VALUE, false);
    }

    /**
     * @param connection
     * @param rowKeys
     * @param isPartialKeys
     * @param maxResults
     */
    public HBaseFetchTask(Connection connection, List<T> rowKeys, boolean isPartialKeys, int maxResults, boolean isFirst) {
        super(connection);
        this.rowKeys = rowKeys;
        this.isPartialKeys = isPartialKeys;
        this.maxResults = maxResults;
        this.isFirst = isFirst;
    }

    /**
     * @param rows
     * @return
     * @throws BlackBoxException
     */
    private List<T> fetch(List<T> rows) throws BlackBoxException {
        if (isPartialKeys) {
            return fetchByPartialKeys(rows);
        }

        List<T> allResults = new ArrayList<>();

        try (Table hTable = getConnection().getTable(utils.getTableName(rows.get(0)))) {
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
        } catch (IOException e) {
            logger.error(e);
        }

        return allResults;
    }

    private List<T> fetchByPartialKeys(List<T> rows) throws BlackBoxException {
        List<T> results = new MaxResultList<>(maxResults);
        final TableName tableName = utils.getTableName(rows.get(0));
        try (Table hTable = getConnection().getTable(tableName)) {
            for (T row : rows) {
                Scan scan = new Scan();

                String rowPrefix = DyDaBoUtils.getStringPrefix(row.getBBRowKey());
                Filter rowFilter = new RowFilter(CompareOperator.EQUAL, new RegexStringComparator(row.getBBRowKey()));
                if (!DyDaBoUtils.isBlankOrNull(rowPrefix)) {
                    scan.setRowPrefixFilter(Bytes.toBytes(rowPrefix));
                }
                logger.debug(rows);
                logger.debug(rowFilter.toString());
                scan.setFilter(rowFilter);

                try (ResultScanner resultScanner = hTable.getScanner(scan)) {
                    for (Result result : resultScanner) {
                        GenericDBTableRow resultTable = utils.parseResultToHTable(result, rows.get(0));

                        T resultObject = new Gson().fromJson(resultTable.toJsonObject(), (Type) rows.get(0).getClass());
                        if (resultObject != null) {
                            results.add(resultObject);
                        }

                        if (isFirst && maxResults > 0 && results.size() >= maxResults) {
                            break;
                        }
                    }
                } catch (UncheckedIOException exception) {
                    return results;
                }
            }
        } catch (IOException ex) {
            logger.error(ex.getMessage(), ex);
        }
        return results;
    }

    @Override
    protected List<T> compute() {
        try {
            return fetch(rowKeys);
        } catch (BlackBoxException e) {
            logger.catching(e);
        }
        return Collections.emptyList();
    }
}
