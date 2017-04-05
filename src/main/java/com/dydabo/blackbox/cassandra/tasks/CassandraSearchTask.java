/*
 * Copyright 2017 viswadas leher <vleher@gmail.com>.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
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
import com.dydabo.blackbox.cassandra.utils.CassandraUtils;
import com.dydabo.blackbox.common.DyDaBoUtils;
import com.dydabo.blackbox.db.obj.GenericDBTableRow;
import com.google.gson.Gson;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ForkJoinTask;
import java.util.concurrent.RecursiveTask;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author viswadas leher <vleher@gmail.com>
 */
public class CassandraSearchTask<T extends BlackBoxable> extends RecursiveTask<List<T>> {

    private static final Logger logger = Logger.getLogger(CassandraSearchTask.class.getName());

    private final Session session;
    private final long maxResults;
    private final List<T> rows;
    private final CassandraUtils<T> utils;

    /**
     *
     * @param session
     * @param row
     * @param maxResults
     */
    public CassandraSearchTask(Session session, T row, long maxResults) {
        this(session, Arrays.asList(row), maxResults);
    }

    /**
     *
     * @param session
     * @param rows
     * @param maxResults
     */
    public CassandraSearchTask(Session session, List<T> rows, long maxResults) {
        this.session = session;
        this.rows = rows;
        this.utils = new CassandraUtils<T>();
        this.maxResults = maxResults;
    }

    public Session getSession() {
        return session;
    }

    /**
     *
     * @param rows
     *
     * @return
     *
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
            List<ForkJoinTask<List<T>>> taskList = new ArrayList();
            for (T row : rows) {
                ForkJoinTask<List<T>> fjTask = new CassandraSearchTask<>(getSession(), Arrays.asList(row), maxResults).fork();
                taskList.add(fjTask);
            }
            // wait for all to join
            for (ForkJoinTask<List<T>> forkJoinTask : taskList) {
                fullResult.addAll(forkJoinTask.join());
            }

            return fullResult;
        }
    }

    protected List<T> search(T row) throws BlackBoxException {
        List<T> results = new ArrayList<>();

        GenericDBTableRow cTable = utils.convertRowToTableRow(row);
        Select selectStmt = QueryBuilder.select().from(utils.getTableName(row));
        selectStmt.enableTracing();
        selectStmt.allowFiltering();

        List<Clause> whereClauses = new ArrayList<>();

        for (Map.Entry<String, GenericDBTableRow.ColumnFamily> family : cTable.getColumnFamilies().entrySet()) {
            String familyName = family.getKey();
            GenericDBTableRow.ColumnFamily familyValue = family.getValue();
            for (Map.Entry<String, GenericDBTableRow.Column> cols : familyValue.getColumns().entrySet()) {
                String colName = cols.getKey();
                GenericDBTableRow.Column colValue = cols.getValue();

                if (colValue != null && colValue.getColumnValue() != null) {
                    final String colString = colValue.getColumnValueAsString();
                    if (DyDaBoUtils.isValidRegex(colString)) {
                        utils.createIndex("\"" + colName + "\"", row);
                        if (DyDaBoUtils.isNumber(colValue.getColumnValue())) {
                            whereClauses.add(QueryBuilder.eq("\"" + colName + "\"", colValue.getColumnValue()));
                        } else {
                            whereClauses.add(QueryBuilder.eq("\"" + colName + "\"", colString));
                        }
                    }
                }
            }
        }

        for (Clause whereClause : whereClauses) {
            selectStmt.where().and(whereClause);
        }

        if (maxResults > 0) {
            selectStmt.limit((int) maxResults);
        }

        ResultSet resultSet = getSession().execute(selectStmt);
        int count = 0;
        for (Row result : resultSet) {
            GenericDBTableRow ctr = new GenericDBTableRow(result.getString("bbkey"));

            for (ColumnDefinitions.Definition def : result.getColumnDefinitions().asList()) {
                final Object object = result.getObject(def.getName());
                if (object != null) {
                    ctr.getDefaultFamily().addColumn(def.getName(), object);
                }
            }

            //resultTable = utils.parseResultToHTable(result, row);
            T resultObject = new Gson().fromJson(ctr.toJsonObject(), (Class<T>) row.getClass());
            if (resultObject != null) {
                results.add(resultObject);
                count++;
            }

            if (maxResults > 0 && count >= maxResults) {
                break;
            }

        }
        return results;
    }

    @Override
    protected List<T> compute() {
        try {
            return search(rows);
        } catch (BlackBoxException ex) {
            logger.log(Level.SEVERE, null, ex);
        }
        return Collections.<T>emptyList();
    }

}
