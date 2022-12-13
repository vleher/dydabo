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

import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.querybuilder.QueryBuilder;
import com.datastax.oss.driver.api.querybuilder.select.Select;
import com.dydabo.blackbox.BlackBoxException;
import com.dydabo.blackbox.BlackBoxable;
import com.dydabo.blackbox.cassandra.db.CassandraConnectionManager;
import com.dydabo.blackbox.cassandra.utils.CassandraConstants;
import com.dydabo.blackbox.cassandra.utils.CassandraUtils;
import com.dydabo.blackbox.common.MaxResultList;
import com.dydabo.blackbox.common.utils.DyDaBoConstants;
import com.dydabo.blackbox.db.obj.GenericDBTableRow;
import com.google.gson.Gson;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ForkJoinTask;
import java.util.concurrent.RecursiveTask;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static com.datastax.oss.driver.api.querybuilder.QueryBuilder.literal;

/**
 * @param <T>
 * @author viswadas leher
 */
public class CassandraFetchTask<T extends BlackBoxable> extends RecursiveTask<List<T>> {

    private final Logger logger = LogManager.getLogger();

    private final boolean isPartialKeys;
    private final int maxResults;
    private final List<T> rowKeys;
    private final CassandraConnectionManager connectionManager;
    private final CassandraUtils<T> utils;
    private final boolean isFirst;

    /**
     * @param connectionManager connection manager
     * @param rows              list of objects to be fetched
     * @param isPartialKeys     if the fetch is using partial keys
     */
    private CassandraFetchTask(CassandraConnectionManager connectionManager, List<T> rows, boolean isPartialKeys) {
        this(connectionManager, rows, isPartialKeys, Integer.MAX_VALUE, false);
    }

    /**
     * @param connectionManager connection manager
     * @param rows              list of objects to be fetched
     * @param isPartialKeys     if the fetch is using partial keys
     * @param maxResults        maximum number of results to return
     */
    public CassandraFetchTask(CassandraConnectionManager connectionManager, List<T> rows, boolean isPartialKeys, int maxResults,
                              boolean isFirst) {
        this.connectionManager = connectionManager;
        this.rowKeys = rows;
        this.utils = new CassandraUtils<>(connectionManager);
        this.isPartialKeys = isPartialKeys;
        this.maxResults = maxResults;
        this.isFirst = isFirst;
    }

    /**
     * @param rows list of objects to fetch
     * @return list of objects
     * @throws BlackBoxException if results cannot be fetched
     */
    private List<T> fetch(List<T> rows) throws BlackBoxException {
        if (rows.size() < DyDaBoConstants.MIN_PARALLEL_THRESHOLD) {
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
            ForkJoinTask<List<T>> fjTask = new CassandraFetchTask<>(getConnectionManager(), Collections.singletonList(row),
                    isPartialKeys, maxResults, isFirst);
            taskList.add(fjTask);
        }
        return invokeAll(taskList).stream().map(ForkJoinTask::join).flatMap(ts -> ts.stream()).collect(Collectors.toList());
    }

    /**
     * @param row
     * @return
     * @throws BlackBoxException
     */
    private List<T> fetch(T row) {

        Select queryStmt = QueryBuilder.selectFrom(utils.getTableName(row)).all();
        if (!isPartialKeys) {
            queryStmt = queryStmt.whereColumn(CassandraConstants.DEFAULT_ROWKEY).isEqualTo(literal(row.getBBRowKey()));
        }

        ResultSet resultSet = getConnectionManager().getSession().execute(queryStmt.build());
        List<T> results = new MaxResultList<>(maxResults);
        for (Row result : resultSet) {
            final String currRowKey = result.getString(CassandraConstants.DEFAULT_ROWKEY);
            boolean isResult = true;
            if (isPartialKeys) {
                isResult = Pattern.matches(row.getBBRowKey(), currRowKey);
            }
            if (isResult) {
                GenericDBTableRow ctr = new GenericDBTableRow(currRowKey);
                result.getColumnDefinitions().forEach(columnDefinition -> ctr.getDefaultFamily().addColumn(columnDefinition.getName().toString(), result.getObject(columnDefinition.getName())));

                T resultObject = new Gson().fromJson(ctr.toJsonObject(), (Type) row.getClass());
                if (resultObject != null) {
                    results.add(resultObject);
                }
            }

            if (isFirst && maxResults > 0 && results.size() >= maxResults) {
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
            logger.error(ex);
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
