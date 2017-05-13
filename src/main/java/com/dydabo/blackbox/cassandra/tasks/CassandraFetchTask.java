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
package com.dydabo.blackbox.cassandra.tasks;

import com.datastax.driver.core.ColumnDefinitions;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.driver.core.querybuilder.Select;
import com.dydabo.blackbox.BlackBoxException;
import com.dydabo.blackbox.BlackBoxable;
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

    private final T bean;
    private final boolean isPartialKeys;
    private final long maxResults;
    private final List<String> rowKeys;
    private final Session session;
    private final CassandraUtils<T> utils;

    /**
     * @param session
     * @param rowKey
     * @param row
     * @param isPartialKeys
     */
    private CassandraFetchTask(Session session, String rowKey, T row, boolean isPartialKeys) {
        this(session, Collections.singletonList(rowKey), row, isPartialKeys);
    }

    /**
     * @param session
     * @param rowKeys
     * @param row
     * @param isPartialKeys
     */
    private CassandraFetchTask(Session session, List<String> rowKeys, T row, boolean isPartialKeys) {
        this(session, rowKeys, row, isPartialKeys, -1);
    }

    /**
     * @param session
     * @param rowKeys
     * @param row
     * @param isPartialKeys
     * @param maxResults
     */
    public CassandraFetchTask(Session session, List<String> rowKeys, T row, boolean isPartialKeys, long maxResults) {
        this.session = session;
        this.rowKeys = rowKeys;
        this.utils = new CassandraUtils<>();
        this.bean = row;
        this.isPartialKeys = isPartialKeys;
        this.maxResults = maxResults;
    }

    /**
     * @param rowKeys
     * @return
     * @throws BlackBoxException
     */
    private List<T> fetch(List<String> rowKeys) throws BlackBoxException {
        if (rowKeys.size() < 2) {
            List<T> fullResult = new ArrayList<>();
            for (String key : rowKeys) {
                fullResult.addAll(fetch(key));
            }
            return fullResult;
        }

        List<T> fullResult = new ArrayList<>();

        // create a task for each element or row in the list
        List<ForkJoinTask<List<T>>> taskList = new ArrayList<>();
        for (String rowKey : rowKeys) {
            ForkJoinTask<List<T>> fjTask = new CassandraFetchTask<>(getSession(), Collections.singletonList(rowKey), bean, isPartialKeys,
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
     * @param rowKey
     * @return
     * @throws BlackBoxException
     */
    private List<T> fetch(String rowKey) throws BlackBoxException {

        Select queryStmt = QueryBuilder.select().from(utils.getTableName(bean)).allowFiltering();
        if (!isPartialKeys) {
            queryStmt.where(QueryBuilder.eq(CassandraConstants.DEFAULT_ROWKEY, rowKey));
        }

        ResultSet resultSet = getSession().execute(queryStmt);
        List<T> results = new ArrayList<>();
        for (Row result : resultSet) {
            final String currRowKey = result.getString(CassandraConstants.DEFAULT_ROWKEY);
            boolean isResult = true;
            if (isPartialKeys) {
                isResult = Pattern.matches(rowKey, currRowKey);
            }
            if (isResult) {
                GenericDBTableRow ctr = new GenericDBTableRow(currRowKey);
                for (ColumnDefinitions.Definition def : result.getColumnDefinitions().asList()) {
                    ctr.getDefaultFamily().addColumn(def.getName(), result.getObject(def.getName()));
                }

                T resultObject = new Gson().fromJson(ctr.toJsonObject(), (Type) bean.getClass());

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
    private Session getSession() {
        return session;
    }

}
