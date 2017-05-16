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
import com.datastax.driver.core.querybuilder.Clause;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.driver.core.querybuilder.Select;
import com.dydabo.blackbox.BlackBoxException;
import com.dydabo.blackbox.BlackBoxable;
import com.dydabo.blackbox.cassandra.utils.CassandraConstants;
import com.dydabo.blackbox.cassandra.utils.CassandraUtils;
import com.dydabo.blackbox.common.DyDaBoUtils;
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

/**
 * @param <T>
 * @author viswadas leher
 */
public class CassandraSearchTask<T extends BlackBoxable> extends RecursiveTask<List<T>> {

    private static final Logger logger = Logger.getLogger(CassandraSearchTask.class.getName());

    private final Session session;
    private final long maxResults;
    private final List<T> rows;
    private final CassandraUtils<T> utils;

    /**
     * @param session
     * @param row
     * @param maxResults
     */
    public CassandraSearchTask(Session session, T row, long maxResults) {
        this(session, Collections.singletonList(row), maxResults);
    }

    /**
     * @param session
     * @param rows
     * @param maxResults
     */
    public CassandraSearchTask(Session session, List<T> rows, long maxResults) {
        this.session = session;
        this.rows = rows;
        this.utils = new CassandraUtils<>();
        this.maxResults = maxResults;
    }

    /**
     * @return
     */
    private Session getSession() {
        return session;
    }

    /**
     * @param rows
     * @return
     * @throws BlackBoxException
     */
    protected List<T> search(List<T> rows) throws BlackBoxException {
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
                ForkJoinTask<List<T>> fjTask = new CassandraSearchTask<>(getSession(), Collections.singletonList(row), maxResults).fork();
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
    protected List<T> search(T row) throws BlackBoxException {
        List<T> results = new ArrayList<>();

        GenericDBTableRow cTable = utils.convertRowToTableRow(row);
        Select selectStmt = QueryBuilder.select().from(utils.getTableName(row));
        selectStmt.enableTracing();
        selectStmt.allowFiltering();

        List<Clause> whereClauses = new ArrayList<>();

        cTable.getColumnFamilies().forEach((familyName, columnFamily) -> {
            columnFamily.getColumns().forEach((columnName, column) -> {
                Object columnValue = column.getColumnValue();
                if (columnValue != null) {
                    String columnValueString = column.getColumnValueAsString();
                    if (DyDaBoUtils.isValidRegex(columnValueString)) {
                        if (DyDaBoUtils.isNumber(columnValue)) {
                            whereClauses.add(QueryBuilder.eq("\"" + columnName + "\"", columnValue));
                        } else {
                            if (DyDaBoUtils.isARegex(columnValueString)) {
                                utils.createIndex(columnName, row);
                                columnValueString = cleanup(columnValueString);
                                whereClauses.add(QueryBuilder.like("\"" + columnName + "\"", columnValueString));
                            } else if (columnValueString.startsWith("[") || columnValueString.startsWith("{")) {
                                // TODO : search inside maps and arrays
                            } else {
                                whereClauses.add(QueryBuilder.eq("\"" + columnName + "\"", columnValueString));
                            }
                        }
                    }
                }
            });
        });

        for (Clause whereClause : whereClauses) {
            selectStmt.where().and(whereClause);
        }

        if (maxResults > 0) {
            selectStmt.limit((int) maxResults);
        }
        logger.finer("Search: " + selectStmt);
        ResultSet resultSet = getSession().execute(selectStmt);
        for (Row result : resultSet) {
            GenericDBTableRow ctr = new GenericDBTableRow(result.getString(CassandraConstants.DEFAULT_ROWKEY));

            for (ColumnDefinitions.Definition def : result.getColumnDefinitions().asList()) {
                final Object object = result.getObject(def.getName());
                if (object != null) {
                    ctr.getDefaultFamily().addColumn(def.getName(), object);
                }
            }

            T resultObject = new Gson().fromJson(ctr.toJsonObject(), (Type) row.getClass());
            if (resultObject != null) {
                results.add(resultObject);
            }

            if (maxResults > 0 && results.size() >= maxResults) {
                break;
            }

        }
        return results;
    }

    private String cleanup(String regexString) {
        String finalString = regexString;
        if (regexString.startsWith("^")) {
            finalString = regexString.substring(1);
        }

        finalString = finalString.replaceAll("\\.\\*", "%");

        return finalString;
    }

    @Override
    protected List<T> compute() {
        try {
            return search(rows);
        } catch (BlackBoxException ex) {
            logger.log(Level.SEVERE, null, ex);
        }
        return Collections.emptyList();
    }

}
