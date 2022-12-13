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
import com.datastax.oss.driver.api.querybuilder.term.Term;
import com.dydabo.blackbox.BlackBoxException;
import com.dydabo.blackbox.BlackBoxable;
import com.dydabo.blackbox.cassandra.db.CassandraConnectionManager;
import com.dydabo.blackbox.cassandra.utils.CassandraConstants;
import com.dydabo.blackbox.cassandra.utils.CassandraUtils;
import com.dydabo.blackbox.common.MaxResultList;
import com.dydabo.blackbox.common.utils.DyDaBoConstants;
import com.dydabo.blackbox.common.utils.DyDaBoUtils;
import com.dydabo.blackbox.db.obj.GenericDBTableRow;
import com.google.gson.Gson;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ForkJoinTask;
import java.util.concurrent.RecursiveTask;
import java.util.stream.Collectors;

import static com.datastax.oss.driver.api.querybuilder.QueryBuilder.literal;

/**
 * @param <T>
 * @author viswadas leher
 */
public class CassandraSearchTask<T extends BlackBoxable> extends RecursiveTask<List<T>> {

    private final Logger logger = LogManager.getLogger();

    private final CassandraConnectionManager connectionManager;
    private final int maxResults;
    private final List<T> rows;
    private final CassandraUtils<T> utils;
    private final boolean isFirst;

    /**
     * @param connectionManager
     * @param row
     * @param maxResults
     */
    public CassandraSearchTask(CassandraConnectionManager connectionManager, T row, int maxResults, boolean isFirst) {
        this(connectionManager, Collections.singletonList(row), maxResults, isFirst);
    }

    /**
     * @param connectionManager
     * @param rows
     * @param maxResults
     */
    public CassandraSearchTask(CassandraConnectionManager connectionManager, List<T> rows, int maxResults, boolean isFirst) {
        this.connectionManager = connectionManager;
        this.rows = rows;
        this.utils = new CassandraUtils<>(connectionManager);
        this.maxResults = maxResults;
        this.isFirst = isFirst;
    }

    /**
     * @return
     */
    private CassandraConnectionManager getConnectionManager() {
        return connectionManager;
    }

    /**
     * @param rows
     * @return
     * @throws BlackBoxException
     */
    private List<T> search(List<T> rows) throws BlackBoxException {
        List<T> fullResult = new ArrayList<>();
        if (rows.size() < DyDaBoConstants.MIN_PARALLEL_THRESHOLD) {
            for (T row : rows) {
                fullResult.addAll(search(row));
            }
        } else {

            // create a task for each element or row in the list
            List<ForkJoinTask<List<T>>> taskList = new ArrayList<>();
            for (T row : rows) {
                ForkJoinTask<List<T>> fjTask = new CassandraSearchTask<>(getConnectionManager(), Collections.singletonList(row),
                        maxResults, isFirst);
                taskList.add(fjTask);
            }

            fullResult =
                    ForkJoinTask.invokeAll(taskList).stream().map(ForkJoinTask::join).flatMap(ts -> ts.stream()).collect(Collectors.toList());
        }
        return fullResult;
    }

    /**
     * @param row
     * @return
     * @throws BlackBoxException
     */
    private List<T> search(T row) {
        List<T> results = new MaxResultList<>(maxResults);
        GenericDBTableRow cTable = utils.convertRowToTableRow(row);
        Select selectStmt = QueryBuilder.selectFrom(utils.getTableName(row)).all().allowFiltering();

        Map<String, Term> whereAndClauses = new HashMap<>();
        Map<String, Term> whereRegexClauses = new HashMap<>();

        cTable.getColumnFamilies().forEach((familyName, columnFamily) -> columnFamily.getColumns().forEach((columnName, column) -> {
            Object columnValue = column.getColumnValue();
            if (columnValue != null) {
                String columnValueString = column.getColumnValueAsString();
                if (DyDaBoUtils.isValidRegex(columnValueString)) {
                    if (DyDaBoUtils.isNumber(columnValue)) {
                        whereAndClauses.put(columnName, literal(columnValue));
                    } else {
                        if (DyDaBoUtils.isARegex(columnValueString)) {
                            utils.createIndex(columnName, row);
                            columnValueString = cleanup(columnValueString);
                            whereRegexClauses.put(columnName, literal(columnValueString));
                        } else if (columnValueString.startsWith("[") || columnValueString.startsWith("{")) {
                            // TODO : search inside maps and arrays
                        } else {
                            whereAndClauses.put(columnName, literal(columnValue));
                        }
                    }
                }
            }
        }));

        whereAndClauses.forEach((s, o) -> selectStmt.whereColumn(s).isEqualTo(o));
        whereRegexClauses.forEach((s, o) -> selectStmt.whereColumn(s).like(o));

        if (maxResults > 0) {
            selectStmt.limit(maxResults);
        }
        logger.debug("Search Query:{}", selectStmt);
        ResultSet resultSet = getConnectionManager().getSession().execute(selectStmt.build());
        for (Row result : resultSet) {
            GenericDBTableRow ctr = new GenericDBTableRow(result.getString(CassandraConstants.DEFAULT_ROWKEY));

            result.getColumnDefinitions().forEach(columnDefinition -> {
                final Object object = result.getObject(columnDefinition.getName());
                if (object != null) {
                    ctr.getDefaultFamily().addColumn(columnDefinition.getName().toString(), object);
                }
            });

            T resultObject = new Gson().fromJson(ctr.toJsonObject(), (Type) row.getClass());
            if (resultObject != null) {
                results.add(resultObject);
            }

            if (isFirst && maxResults > 0 && results.size() >= maxResults) {
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
            logger.error(ex);
        }
        return Collections.emptyList();
    }
}
