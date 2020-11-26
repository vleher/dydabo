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
package com.dydabo.blackbox.cassandra.tasks;

import com.datastax.driver.core.ColumnDefinitions;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.driver.core.querybuilder.Select;
import com.dydabo.blackbox.BlackBoxException;
import com.dydabo.blackbox.BlackBoxable;
import com.dydabo.blackbox.cassandra.db.CassandraConnectionManager;
import com.dydabo.blackbox.cassandra.utils.CassandraConstants;
import com.dydabo.blackbox.cassandra.utils.CassandraUtils;
import com.dydabo.blackbox.db.obj.GenericDBTableRow;
import com.google.gson.Gson;

import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ForkJoinTask;
import java.util.concurrent.RecursiveTask;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.regex.Pattern;

/**
 * @param <T>
 * @author viswadas leher
 */
public class CassandraFetchTask<T extends BlackBoxable> extends RecursiveTask<List<T>> {

    private static final Logger logger = Logger.getLogger(CassandraFetchTask.class.getName());

    private final boolean isPartialKeys;
    private final long maxResults;
    private final List<T> rowKeys;
    private final CassandraConnectionManager connectionManager;
    private final CassandraUtils<T> utils;


    /**
     * @param connectionManager
     * @param rows
     * @param isPartialKeys
     */
    private CassandraFetchTask(CassandraConnectionManager connectionManager, List<T> rows, boolean isPartialKeys) {
        this(connectionManager, rows, isPartialKeys, -1);
    }

    /**
     * @param connectionManager
     * @param rows
     * @param isPartialKeys
     * @param maxResults
     */
    public CassandraFetchTask(CassandraConnectionManager connectionManager, List<T> rows, boolean isPartialKeys, long maxResults) {
        this.connectionManager = connectionManager;
        this.rowKeys = rows;
        this.utils = new CassandraUtils<>(connectionManager);
        this.isPartialKeys = isPartialKeys;
        this.maxResults = maxResults;
    }

    /**
     * @param rows
     * @return
     * @throws BlackBoxException
     */
    private List<T> fetch(List<T> rows) throws BlackBoxException {
        if (rows.size() < 2) {
            List<T> fullResult = new ArrayList<>();
            for (T row : rows) {
                fullResult.addAll(fetch(row));
            }
            return fullResult;
        }

        List<T> fullResult = new ArrayList<>();

        // create a task for each element or row in the list
        List<ForkJoinTask<List<T>>> taskList = new ArrayList<>();
        for (T row : rows) {
            ForkJoinTask<List<T>> fjTask = new CassandraFetchTask<T>(getConnectionManager(), Collections.singletonList(row), isPartialKeys,
                    maxResults).fork();
            taskList.add(fjTask);
        }
        // wait for all to join
        for (ForkJoinTask<List<T>> forkJoinTask : taskList) {
            fullResult.addAll(forkJoinTask.join());
        }

        return fullResult;

    }

    /**
     * @param row
     * @return
     * @throws BlackBoxException
     */
    private List<T> fetch(T row) {

        Select queryStmt = QueryBuilder.select().from(utils.getTableName(row)).allowFiltering();
        if (!isPartialKeys) {
            queryStmt.where(QueryBuilder.eq(CassandraConstants.DEFAULT_ROWKEY, row));
        }

        ResultSet resultSet = getConnectionManager().getSession().execute(queryStmt);
        List<T> results = new ArrayList<>();
        for (Row result : resultSet) {
            final String currRowKey = result.getString(CassandraConstants.DEFAULT_ROWKEY);
            boolean isResult = true;
            if (isPartialKeys) {
                isResult = Pattern.matches(row.getBBRowKey(), currRowKey);
            }
            if (isResult) {
                GenericDBTableRow ctr = new GenericDBTableRow(currRowKey);
                for (ColumnDefinitions.Definition def : result.getColumnDefinitions().asList()) {
                    ctr.getDefaultFamily().addColumn(def.getName(), result.getObject(def.getName()));
                }

                T resultObject = new Gson().fromJson(ctr.toJsonObject(), (Type) row.getClass());

                if (resultObject != null) {
                    results.add(resultObject);
                }
            }

            if (maxResults > 0 && results.size() >= maxResults) {
                break;
            }

        }
        return results;
    }

    @Override
    protected List<T> compute() {
        try {
            return fetch(rowKeys);
        } catch (BlackBoxException ex) {
            logger.log(Level.SEVERE, null, ex);
        }
        return Collections.emptyList();
    }

    /**
     * @return
     */
    private CassandraConnectionManager getConnectionManager() {
        return connectionManager;
    }

}
