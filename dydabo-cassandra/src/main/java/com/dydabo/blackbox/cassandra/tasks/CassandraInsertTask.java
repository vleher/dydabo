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
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.querybuilder.QueryBuilder;
import com.datastax.oss.driver.api.querybuilder.insert.InsertInto;
import com.datastax.oss.driver.api.querybuilder.term.Term;
import com.dydabo.blackbox.BlackBoxException;
import com.dydabo.blackbox.BlackBoxable;
import com.dydabo.blackbox.cassandra.db.CassandraConnectionManager;
import com.dydabo.blackbox.cassandra.utils.CassandraConstants;
import com.dydabo.blackbox.cassandra.utils.CassandraUtils;
import com.dydabo.blackbox.common.utils.DyDaBoConstants;
import com.dydabo.blackbox.common.utils.DyDaBoUtils;
import com.dydabo.blackbox.db.obj.GenericDBTableRow;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ForkJoinTask;
import java.util.concurrent.RecursiveTask;

import static com.datastax.oss.driver.api.querybuilder.QueryBuilder.literal;

/**
 * @param <T>
 * @author viswadas leher
 */
public class CassandraInsertTask<T extends BlackBoxable> extends RecursiveTask<Boolean> {

    private final Logger logger = LogManager.getLogger();

    private final boolean checkExisting;
    private final CassandraConnectionManager connectionManager;
    private final List<T> rows;
    private final CassandraUtils<T> utils;

    /**
     * @param connectionManager
     * @param row
     * @param checkExisting
     */
    private CassandraInsertTask(CassandraConnectionManager connectionManager, T row, boolean checkExisting) {
        this(connectionManager, Collections.singletonList(row), checkExisting);
    }

    /**
     * @param connectionManager
     * @param rows
     * @param checkExisting
     */
    public CassandraInsertTask(CassandraConnectionManager connectionManager, List<T> rows, boolean checkExisting) {
        this.connectionManager = connectionManager;
        this.rows = rows;
        this.checkExisting = checkExisting;
        this.utils = new CassandraUtils<>(connectionManager);
    }

    @Override
    protected Boolean compute() {
        try {
            return insert(rows, checkExisting);
        } catch (BlackBoxException ex) {
            logger.error(ex);
        }
        return false;
    }

    /**
     * @return
     */
    private CassandraConnectionManager getConnectionManager() {
        return connectionManager;
    }

    /**
     * @param rows
     * @param checkExisting
     * @return
     * @throws BlackBoxException
     */
    private Boolean insert(List<T> rows, boolean checkExisting) throws BlackBoxException {
        if (rows.size() < DyDaBoConstants.MIN_PARALLEL_THRESHOLD) {
            Boolean successFlag = Boolean.TRUE;
            for (T t : rows) {
                successFlag = successFlag && insert(t, checkExisting);
            }
            return successFlag;
        }

        Boolean successFlag = Boolean.TRUE;
        // create a task for each element or row in the list
        List<ForkJoinTask<Boolean>> taskList = new ArrayList<>();
        for (T row : rows) {
            ForkJoinTask<Boolean> fjTask = new CassandraInsertTask<>(getConnectionManager(), Collections.singletonList(row),
                    checkExisting);
            taskList.add(fjTask);
        }
        return ForkJoinTask.invokeAll(taskList).stream().map(ForkJoinTask::join).reduce(Boolean::logicalAnd).orElse(false);
    }

    /**
     * @param row
     * @param checkExisting
     * @return
     * @throws BlackBoxException
     */
    private Boolean insert(T row, boolean checkExisting) {
        boolean successFlag = true;

        InsertInto insStmt = QueryBuilder.insertInto(CassandraConstants.KEYSPACE, utils.getTableName(row));

        GenericDBTableRow cTable = utils.convertRowToTableRow(row);

        Map<String, Term> fields = new HashMap<>();
        fields.put(CassandraConstants.DEFAULT_ROWKEY, literal(row.getBBRowKey()));

        cTable.forEach((familyName, columnName, columnValue, columnValueAsString) -> {
            if (columnValue != null) {
                if (DyDaBoUtils.isNumber(columnValue)) {
                    fields.put(columnName, literal(columnValue));
                } else {
                    fields.put(columnName, literal(columnValueAsString));
                }
            }
        });

        SimpleStatement statement;
        if (checkExisting) {
            statement = insStmt.values(fields).ifNotExists().build();
        } else {
            statement = insStmt.values(fields).build();
        }

        logger.debug("Executing query: {}", statement.getQuery());
        // execute query, might throw exception
        ResultSet result = getConnectionManager().getSession().execute(statement);

        result.forEach(r -> logger.debug(r.toString()));

        return Objects.nonNull(result);
    }
}
