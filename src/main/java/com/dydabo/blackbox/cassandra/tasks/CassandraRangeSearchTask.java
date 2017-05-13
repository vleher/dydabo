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
import com.dydabo.blackbox.BlackBoxable;
import com.dydabo.blackbox.cassandra.utils.CassandraConstants;
import com.dydabo.blackbox.cassandra.utils.CassandraUtils;
import com.dydabo.blackbox.common.DyDaBoUtils;
import com.dydabo.blackbox.db.obj.GenericDBTableRow;
import com.google.gson.Gson;

import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.RecursiveTask;
import java.util.logging.Logger;

/**
 * @param <T>
 * @author viswadas leher
 */
public class CassandraRangeSearchTask<T extends BlackBoxable> extends RecursiveTask<List<T>> {

    private static final Logger logger = Logger.getLogger(CassandraRangeSearchTask.class.getName());
    private final long maxResults;
    private final Session session;
    private final T endRow;
    private final T startRow;
    private final CassandraUtils<BlackBoxable> utils;

    /**
     * @param session
     * @param startRow
     * @param endRow
     * @param maxResults
     */
    public CassandraRangeSearchTask(Session session, T startRow, T endRow, long maxResults) {
        this.session = session;
        this.startRow = startRow;
        this.endRow = endRow;
        this.maxResults = maxResults;
        this.utils = new CassandraUtils<>();
    }

    @Override
    protected List<T> compute() {
        List<T> results = new ArrayList<>();
        Select queryStmt = QueryBuilder.select().from(CassandraConstants.KEYSPACE, utils.getTableName(startRow));
        queryStmt.allowFiltering();

        GenericDBTableRow startTableRow = utils.convertRowToTableRow(startRow);
        GenericDBTableRow endTableRow = utils.convertRowToTableRow(endRow);

        List<Clause> whereClauses = new ArrayList<>();
        // parse the start row for where clauses
        parseClausesStart(startTableRow, whereClauses);
        // parse the end row for where clauses
        parseClausesEnd(endTableRow, whereClauses);

        for (Clause whereClause : whereClauses) {
            queryStmt.where().and(whereClause);
        }
        logger.info("Range Search :" + queryStmt);
        ResultSet resultSet = getSession().execute(queryStmt);
        for (Row result : resultSet) {
            GenericDBTableRow ctr = new GenericDBTableRow(result.getString(CassandraConstants.DEFAULT_ROWKEY));
            for (ColumnDefinitions.Definition def : result.getColumnDefinitions().asList()) {
                ctr.getDefaultFamily().addColumn(def.getName(), result.getObject(def.getName()));
            }

            T resultObject = new Gson().fromJson(ctr.toJsonObject(), (Type) startRow.getClass());
            if (resultObject != null) {
                if (maxResults < 0) {
                    results.add(resultObject);
                } else if (maxResults > 0 && results.size() < maxResults) {
                    results.add(resultObject);
                } else {
                    break;
                }
            }

        }
        return results;
    }

    /**
     * @return
     */
    private Session getSession() {
        return session;
    }

    private void parseClausesEnd(GenericDBTableRow endTableRow, List<Clause> whereClauses) {
        for (Map.Entry<String, GenericDBTableRow.ColumnFamily> fam : endTableRow.getColumnFamilies().entrySet()) {
            GenericDBTableRow.ColumnFamily colFamily = fam.getValue();

            for (Map.Entry<String, GenericDBTableRow.Column> cols : colFamily.getColumns().entrySet()) {
                String colName = cols.getKey();
                GenericDBTableRow.Column column = cols.getValue();

                if (column != null && column.getColumnValue() != null) {
                    final String colString = column.getColumnValueAsString();
                    if (DyDaBoUtils.isValidRegex(colString)) {
                        if (DyDaBoUtils.isNumber(column.getColumnValue())) {
                            whereClauses.add(QueryBuilder.lt("\"" + colName + "\"", column.getColumnValue()));
                        } else {
                            if (DyDaBoUtils.isARegex(colString)) {
                                utils.createIndex(colName, startRow);
                                whereClauses.add(QueryBuilder.like("\"" + colName + "\"", cleanup(colString)));
                            } else {
                                whereClauses.add(QueryBuilder.lt("\"" + colName + "\"", colString));
                            }
                        }
                    }
                }
            }
        }
    }

    private void parseClausesStart(GenericDBTableRow startTableRow, List<Clause> whereClauses) {
        for (Map.Entry<String, GenericDBTableRow.ColumnFamily> fam : startTableRow.getColumnFamilies().entrySet()) {
            GenericDBTableRow.ColumnFamily colFamily = fam.getValue();

            for (Map.Entry<String, GenericDBTableRow.Column> cols : colFamily.getColumns().entrySet()) {
                String colName = cols.getKey();
                GenericDBTableRow.Column column = cols.getValue();

                if (column != null && column.getColumnValue() != null) {
                    final String colString = column.getColumnValueAsString();
                    if (DyDaBoUtils.isValidRegex(colString)) {
                        if (DyDaBoUtils.isNumber(column.getColumnValue())) {
                            whereClauses.add(QueryBuilder.gte("\"" + colName + "\"", column.getColumnValue()));
                        } else {
                            if (DyDaBoUtils.isARegex(colString)) {
                                utils.createIndex(colName, startRow);
                                whereClauses.add(QueryBuilder.like("\"" + colName + "\"", cleanup(colString)));
                            } else {
                                whereClauses.add(QueryBuilder.gte("\"" + colName + "\"", colString));
                            }
                        }
                    }
                }

            }

        }
    }
    private String cleanup(String regexString) {
        String finalString = regexString;
        if (regexString.startsWith("^")) {
            finalString = regexString.substring(1);
        }

        finalString = finalString.replaceAll("\\.\\*", "%");

        return finalString;
    }
}
