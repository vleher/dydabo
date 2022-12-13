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

import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.querybuilder.QueryBuilder;
import com.dydabo.blackbox.BlackBoxable;
import com.dydabo.blackbox.cassandra.db.CassandraConnectionManager;
import com.dydabo.blackbox.cassandra.utils.CassandraConstants;
import com.dydabo.blackbox.cassandra.utils.CassandraUtils;
import com.dydabo.blackbox.common.utils.DyDaBoConstants;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ForkJoinTask;
import java.util.concurrent.RecursiveTask;

import static com.datastax.oss.driver.api.querybuilder.QueryBuilder.bindMarker;
import static com.datastax.oss.driver.api.querybuilder.QueryBuilder.literal;

/**
 * Delete rows from Cassandra
 *
 * @param <T> the object (row) being processed
 * @author viswadas leher
 */
public class CassandraDeleteTask<T extends BlackBoxable> extends RecursiveTask<Boolean> {

    private final Logger logger = LogManager.getLogger();

    private final CassandraConnectionManager connectionManager;
    private final List<T> rows;
    private final CassandraUtils<T> utils;

    /**
     * Constructor
     *
     * @param connectionManager a connectionManager instance for db connection
     * @param rows              list of rows to be deleted
     */
    public CassandraDeleteTask(CassandraConnectionManager connectionManager, List<T> rows) {
        this.connectionManager = connectionManager;
        this.rows = rows;
        this.utils = new CassandraUtils<>(connectionManager);
    }

    @Override
    protected Boolean compute() {
        return delete(rows);
    }

    /**
     * Delete multiple rows from the database concurrently.
     *
     * @param rows rows to be deleted
     * @return true if all operations were successful
     */
    private Boolean delete(List<T> rows) {
        // one row is the recursion base line
        if (rows.size() < DyDaBoConstants.MIN_PARALLEL_THRESHOLD) {
            boolean successFlag = true;
            for (T t : rows) {
                successFlag = successFlag && delete(t);
            }
            return successFlag;
        }

        Boolean successFlag = Boolean.TRUE;
        // create a task for each element or row in the list
        List<ForkJoinTask<Boolean>> taskList = new ArrayList<>();
        for (T row : rows) {
            ForkJoinTask<Boolean> fjTask = new CassandraDeleteTask<>(getConnectionManager(), Collections.singletonList(row));
            taskList.add(fjTask);
        }

        return ForkJoinTask.invokeAll(taskList).stream().map(ForkJoinTask::join).reduce(Boolean::logicalAnd).orElse(false);
    }

    /**
     * Delete a single row from the database
     *
     * @param row the row to be deleted
     * @return true if the delete was successful
     */
    private Boolean delete(T row) {
        // Create a delete statement with the row key
        SimpleStatement delStmt =
                QueryBuilder.deleteFrom(CassandraConstants.KEYSPACE, utils.getTableName(row)).whereColumn(CassandraConstants.DEFAULT_ROWKEY).isEqualTo(bindMarker()).ifColumn(CassandraConstants.DEFAULT_ROWKEY).isEqualTo(literal(row.getBBRowKey())).build();

        getConnectionManager().getSession().execute(delStmt);

        return true;
    }

    /**
     * @return connection manager
     */
    private CassandraConnectionManager getConnectionManager() {
        return connectionManager;
    }
}
