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

import com.datastax.driver.core.Session;
import com.datastax.driver.core.querybuilder.Insert;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.dydabo.blackbox.BlackBoxException;
import com.dydabo.blackbox.BlackBoxable;
import com.dydabo.blackbox.cassandra.utils.CassandraConstants;
import com.dydabo.blackbox.cassandra.utils.CassandraUtils;
import com.dydabo.blackbox.common.utils.DyDaBoUtils;
import com.dydabo.blackbox.db.obj.GenericDBTableRow;

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
public class CassandraInsertTask<T extends BlackBoxable> extends RecursiveTask<Boolean> {

    private static final Logger logger = Logger.getLogger(CassandraInsertTask.class.getName());

    private final boolean checkExisting;
    private final Session session;
    private final List<T> rows;
    private final CassandraUtils<T> utils;

    /**
     * @param session
     * @param row
     * @param checkExisting
     */
    private CassandraInsertTask(Session session, T row, boolean checkExisting) {
        this(session, Collections.singletonList(row), checkExisting);
    }

    /**
     * @param session
     * @param rows
     * @param checkExisting
     */
    public CassandraInsertTask(Session session, List<T> rows, boolean checkExisting) {
        this.session = session;
        this.rows = rows;
        this.checkExisting = checkExisting;
        this.utils = new CassandraUtils<>();
    }

    @Override
    protected Boolean compute() {
        try {
            return insert(rows, checkExisting);
        } catch (BlackBoxException ex) {
            logger.log(Level.SEVERE, null, ex);
        }
        return false;
    }

    /**
     * @return
     */
    private Session getSession() {
        return session;
    }

    /**
     * @param rows
     * @param checkExisting
     * @return
     * @throws BlackBoxException
     */
    private Boolean insert(List<T> rows, boolean checkExisting) throws BlackBoxException {
        if (rows.size() < 2) {
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
            ForkJoinTask<Boolean> fjTask = new CassandraInsertTask<>(getSession(), Collections.singletonList(row), checkExisting).fork();
            taskList.add(fjTask);
        }
        // wait for all to join
        for (ForkJoinTask<Boolean> forkJoinTask : taskList) {
            successFlag = successFlag && forkJoinTask.join();
        }

        return successFlag;

    }

    /**
     * @param row
     * @param checkExisting
     * @return
     * @throws BlackBoxException
     */
    private Boolean insert(T row, boolean checkExisting) {
        boolean successFlag = true;

        final Insert insStmt = QueryBuilder.insertInto(CassandraConstants.KEYSPACE, utils.getTableName(row));

        if (checkExisting) {
            insStmt.ifNotExists();
        }

        GenericDBTableRow cTable = utils.convertRowToTableRow(row);
        insStmt.value("\"" + CassandraConstants.DEFAULT_ROWKEY + "\"", row.getBBRowKey());

        cTable.forEach((familyName, columnName, columnValue, columnValueAsString) -> {
            if (columnValue != null) {
                if (DyDaBoUtils.isNumber(columnValue)) {
                    insStmt.value("\"" + columnName + "\"", columnValue);
                } else {
                    insStmt.value("\"" + columnName + "\"", columnValueAsString);
                }
            }
        });

        //try (Session session = CassandraConnectionManager.getSession()) {
        logger.info("Executing " + insStmt.toString());
        // execute query, might throw exception
        session.execute(insStmt);
        //}

        return successFlag;
    }

}
