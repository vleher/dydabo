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
import com.dydabo.blackbox.BlackBoxable;
import com.dydabo.blackbox.cassandra.db.CassandraConnectionManager;
import com.dydabo.blackbox.cassandra.utils.CassandraConstants;
import com.dydabo.blackbox.cassandra.utils.CassandraUtils;
import com.dydabo.blackbox.common.MaxResultList;
import com.dydabo.blackbox.common.utils.DyDaBoUtils;
import com.dydabo.blackbox.db.obj.GenericDBTableRow;
import com.google.gson.Gson;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.lang.reflect.Type;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.RecursiveTask;

import static com.datastax.oss.driver.api.querybuilder.QueryBuilder.literal;

/**
 * @param <T>
 * @author viswadas leher
 */
public class CassandraRangeSearchTask<T extends BlackBoxable> extends RecursiveTask<List<T>> {

    private final Logger logger = LogManager.getLogger();
    private final int maxResults;
    private final CassandraConnectionManager connectionManager;
    private final T endRow;
    private final T startRow;
    private final CassandraUtils<BlackBoxable> utils;
    private final boolean isFirst;

    /**
     * @param connectionManager
     * @param startRow
     * @param endRow
     * @param maxResults
     */
    public CassandraRangeSearchTask(CassandraConnectionManager connectionManager, T startRow, T endRow, int maxResults,
                                    boolean isFirst) {
        this.connectionManager = connectionManager;
        this.startRow = startRow;
        this.endRow = endRow;
        this.maxResults = maxResults;
        this.isFirst = isFirst;
        this.utils = new CassandraUtils<>(connectionManager);
    }

    @Override
    protected List<T> compute() {
        List<T> results = new MaxResultList<>(maxResults);
        Select queryStmt =
                QueryBuilder.selectFrom(CassandraConstants.KEYSPACE, utils.getTableName(startRow)).all().allowFiltering();

        GenericDBTableRow startTableRow = utils.convertRowToTableRow(startRow);
        GenericDBTableRow endTableRow = utils.convertRowToTableRow(endRow);

        Map<String, Term> whereClauses = new HashMap<>();
        // parse the start row for where clauses
        parseClausesStart(startTableRow, queryStmt);
        // parse the end row for where clauses
        parseClausesEnd(endTableRow, queryStmt);

        logger.debug("Range Search :{}", queryStmt);
        ResultSet resultSet = getConnectionManager().getSession().execute(queryStmt.build());
        for (Row result : resultSet) {
            GenericDBTableRow ctr = new GenericDBTableRow(result.getString(CassandraConstants.DEFAULT_ROWKEY));
            result.getColumnDefinitions().forEach(columnDefinition -> ctr.getDefaultFamily().addColumn(columnDefinition.getName().toString(), result.getObject(columnDefinition.getName())));

            T resultObject = new Gson().fromJson(ctr.toJsonObject(), (Type) startRow.getClass());
            if (resultObject != null) {
                results.add(resultObject);
                if (isFirst && maxResults > 0 && results.size() < maxResults) {
                    break;
                }
            }
        }
        return results;
    }

    /**
     * @return
     */
    private CassandraConnectionManager getConnectionManager() {
        return connectionManager;
    }

    private void parseClausesEnd(GenericDBTableRow endTableRow, Select select) {

        endTableRow.forEach((familyName, columnName, columnValue, columnValueAsString) -> {
            if (DyDaBoUtils.isValidRegex(columnValueAsString)) {
                if (DyDaBoUtils.isNumber(columnValue)) {
                    select.whereColumn(columnName).isLessThan(literal(columnValue));
                } else {
                    if (DyDaBoUtils.isARegex(columnValueAsString)) {
                        utils.createIndex(columnName, startRow);
                        select.whereColumn(columnName).like(literal(cleanup(columnValueAsString)));
                    } else {
                        select.whereColumn(columnName).isLessThan(literal(columnValue));
                    }
                }
            }
        });

    }

    private void parseClausesStart(GenericDBTableRow startTableRow, Select select) {

        startTableRow.forEach((familyName, columnName, columnValue, columnValueAsString) -> {
            if (DyDaBoUtils.isValidRegex(columnValueAsString)) {
                if (DyDaBoUtils.isNumber(columnValue)) {
                    select.whereColumn(columnName).isGreaterThan(literal(columnValue));
                } else {
                    if (DyDaBoUtils.isARegex(columnValueAsString)) {
                        utils.createIndex(columnName, startRow);
                        select.whereColumn(columnName).like(literal(cleanup(columnValueAsString)));
                    } else {
                        select.whereColumn(columnName).isGreaterThan(literal(columnValue));
                    }
                }
            }
        });
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
