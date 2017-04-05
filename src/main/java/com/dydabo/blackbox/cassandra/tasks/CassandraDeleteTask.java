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

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.querybuilder.Delete;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.dydabo.blackbox.BlackBoxException;
import com.dydabo.blackbox.BlackBoxable;
import com.dydabo.blackbox.cassandra.utils.CassandraUtils;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.RecursiveTask;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author viswadas leher <vleher@gmail.com>
 */
public class CassandraDeleteTask<T extends BlackBoxable> extends RecursiveTask<Boolean> {

    private static final Logger logger = Logger.getLogger(CassandraDeleteTask.class.getName());

    private final Session session;
    private final List<T> rows;
    private final CassandraUtils<T> utils;

    /**
     *
     * @param connection
     * @param row
     */
    public CassandraDeleteTask(Session session, T row) {
        this(session, Arrays.asList(row));
    }

    /**
     *
     * @param connection
     * @param rows
     */
    public CassandraDeleteTask(Session session, List<T> rows) {
        this.session = session;
        this.rows = rows;
        this.utils = new CassandraUtils<T>();
    }

    @Override
    protected Boolean compute() {
        try {
            return delete(rows);
        } catch (BlackBoxException ex) {
            logger.log(Level.SEVERE, null, ex);
        }
        return false;
    }

    /**
     *
     * @param rows
     *
     * @return
     *
     * @throws BlackBoxException
     */
    protected Boolean delete(List<T> rows) throws BlackBoxException {
        if (rows.size() < 2) {
            Boolean successFlag = true;
            for (T t : rows) {
                successFlag = successFlag && delete(t);
            }
            return successFlag;
        } else {
            Boolean successFlag = Boolean.TRUE;
            int mid = rows.size() / 2;
            CassandraDeleteTask<T> fDeleteJob = new CassandraDeleteTask<>(getSession(), rows.subList(0, mid));
            CassandraDeleteTask<T> sDeleteJob = new CassandraDeleteTask<>(getSession(), rows.subList(mid, rows.size()));
            fDeleteJob.fork();
            Boolean secondFlag = sDeleteJob.compute();
            Boolean firstFlag = fDeleteJob.join();
            successFlag = successFlag && secondFlag && firstFlag;
            return successFlag;
        }
    }

    /**
     *
     * @param row
     *
     * @return
     *
     * @throws BlackBoxException
     */
    protected Boolean delete(T row) throws BlackBoxException {

        Delete delStmt = QueryBuilder.delete().from("bb", utils.getTableName(row));
        delStmt.where(QueryBuilder.eq("\"bbkey\"", row.getBBRowKey()));

        Session session = getSession();

        ResultSet resultSet = session.execute(delStmt);
        //TODO: verify results
        return true;
    }

    public Session getSession() {
        return session;
    }
}
