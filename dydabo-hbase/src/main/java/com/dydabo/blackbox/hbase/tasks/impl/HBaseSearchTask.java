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
import com.dydabo.blackbox.hbase.HBaseBlackBoxImpl;
import com.dydabo.blackbox.hbase.tasks.HBaseTask;
import com.google.gson.Gson;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ForkJoinTask;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * @param <T>
 * @author viswadas leher
 */
public class HBaseSearchTask<T extends BlackBoxable> extends HBaseTask<T> {

    private final Logger logger = Logger.getLogger(HBaseBlackBoxImpl.class.getName());
    private final List<T> rows;
    private final long maxResults;

    /**
     * @param connection
     * @param row
     * @param maxResults
     */
    public HBaseSearchTask(Connection connection, T row, long maxResults) {
        this(connection, Collections.singletonList(row), maxResults);
    }

    /**
     * @param connection
     * @param rows
     * @param maxResults
     */
    public HBaseSearchTask(Connection connection, List<T> rows, long maxResults) {
        super(connection);
        this.rows = rows;
        this.maxResults = maxResults;
    }

    @Override
    protected List<T> compute() {
        try {
            return search(rows);
        } catch (BlackBoxException ex) {
            logger.log(Level.SEVERE, null, ex);
        }
        return new ArrayList<>();
    }

    /**
     * @param thisTable
     * @param filterList
     * @return
     */
    private boolean parseForFilters(GenericDBTableRow thisTable, FilterList filterList) {
        final boolean[] hasFilters = {false};

        thisTable.forEach((familyName, columnName, columnValue, columnValueAsString) -> {
            String regexValue = utils.sanitizeRegex(columnValueAsString);
            if (regexValue != null && DyDaBoUtils.isValidRegex(regexValue)) {
                if (DyDaBoUtils.isNumber(columnValue)) {
                    BinaryComparator regexComp = new BinaryComparator(utils.getAsByteArray(columnValue));
                    SingleColumnValueFilter scvf = new SingleColumnValueFilter(Bytes.toBytes(familyName),
                            Bytes.toBytes(columnName), CompareFilter.CompareOp.EQUAL, regexComp);
                    filterList.addFilter(scvf);
                    hasFilters[0] = true;
                } else {
                    RegexStringComparator regexComp = new RegexStringComparator(regexValue);
                    SingleColumnValueFilter scvf = new SingleColumnValueFilter(Bytes.toBytes(familyName),
                            Bytes.toBytes(columnName), CompareFilter.CompareOp.EQUAL, regexComp);
                    filterList.addFilter(scvf);
                    hasFilters[0] = true;
                }
            }
        });

        return hasFilters[0];
    }

    /**
     * @param rows
     * @return
     * @throws BlackBoxException
     */
    private List<T> search(List<T> rows) throws BlackBoxException {
        if (rows.size() < 2) {
            List<T> fullResult = new ArrayList<>();
            for (T row : rows) {
                fullResult.addAll(search(row));
            }
            return fullResult;
        } else {
            List<T> fullResult = new ArrayList<>();

            // create a task for each element or row in the list
            List<ForkJoinTask<List<T>>> taskList = new ArrayList<>();
            for (T row : rows) {
                ForkJoinTask<List<T>> fjTask = new HBaseSearchTask<>(getConnection(), Collections.singletonList(row), maxResults).fork();
                taskList.add(fjTask);
            }
            // wait for all to join
            for (ForkJoinTask<List<T>> forkJoinTask : taskList) {
                fullResult.addAll(forkJoinTask.join());
            }

            return fullResult;
        }
    }

    /**
     * @param row
     * @return
     * @throws BlackBoxException
     */
    private List<T> search(T row) {
        List<T> results = new ArrayList<>();

        try (Admin admin = getConnection().getAdmin()) {
            // consider create to be is nothing but alter...so
            try (Table hTable = admin.getConnection().getTable(utils.getTableName(row))) {
                Scan scan = new Scan();
                if (maxResults > 0) {
                    scan.setMaxResultSize(maxResults);
                }
                // Get the filters : just simple regex filters for now
                GenericDBTableRow thisTable = utils.convertRowToTableRow(row);
                FilterList filterList = new FilterList(FilterList.Operator.MUST_PASS_ALL);
                boolean hasFilters = parseForFilters(thisTable, filterList);
                if (hasFilters) {
                    logger.finest("Filters:" + filterList);
                    scan.setFilter(filterList);
                }

                try (ResultScanner resultScanner = hTable.getScanner(scan)) {
                    int count = 0;
                    for (Result result : resultScanner) {
                        GenericDBTableRow resultTable = utils.parseResultToHTable(result, row);

                        T resultObject = new Gson().fromJson(resultTable.toJsonObject(), (Type) row.getClass());
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
        } catch (IOException ex) {
            logger.log(Level.SEVERE, null, ex);
        }
        return results;
    }

}
