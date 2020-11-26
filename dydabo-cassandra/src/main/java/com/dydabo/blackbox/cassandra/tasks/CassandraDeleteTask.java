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

import com.datastax.driver.core.querybuilder.Delete;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.dydabo.blackbox.BlackBoxable;
import com.dydabo.blackbox.cassandra.db.CassandraConnectionManager;
import com.dydabo.blackbox.cassandra.utils.CassandraConstants;
import com.dydabo.blackbox.cassandra.utils.CassandraUtils;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ForkJoinTask;
import java.util.concurrent.RecursiveTask;
import java.util.logging.Logger;

/**
 * Delete rows from Cassandra
 *
 * @param <T> the object (row) being processed
 * @author viswadas leher
 */
public class CassandraDeleteTask<T extends BlackBoxable> extends RecursiveTask<Boolean> {

    private static final Logger logger = Logger.getLogger(CassandraDeleteTask.class.getName());

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
        if (rows.size() < 2) {
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
            ForkJoinTask<Boolean> fjTask = new CassandraDeleteTask<>(getConnectionManager(), Collections.singletonList(row)).fork();
            taskList.add(fjTask);
        }
        // wait for all to join
        for (ForkJoinTask<Boolean> forkJoinTask : taskList) {
            successFlag = successFlag && forkJoinTask.join();
        }
        // return the overall status
        return successFlag;
    }

    /**
     * Delete a single row from the database
     *
     * @param row the row to be deleted
     * @return true if the delete was successful
     */
    private Boolean delete(T row) {
        logger.info("deleting " + row);
        // Create a delete statement with the row key
        Delete delStmt = QueryBuilder.delete().from(CassandraConstants.KEYSPACE, utils.getTableName(row));
        delStmt.where(QueryBuilder.eq(CassandraConstants.DEFAULT_ROWKEY, row.getBBRowKey()));

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
