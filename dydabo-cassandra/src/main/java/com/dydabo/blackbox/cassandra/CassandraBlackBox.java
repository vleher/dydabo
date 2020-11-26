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
package com.dydabo.blackbox.cassandra;

import com.dydabo.blackbox.BlackBox;
import com.dydabo.blackbox.BlackBoxException;
import com.dydabo.blackbox.BlackBoxable;
import com.dydabo.blackbox.cassandra.db.CassandraConnectionManager;
import com.dydabo.blackbox.cassandra.tasks.*;
import com.dydabo.blackbox.cassandra.utils.CassandraUtils;
import com.dydabo.blackbox.common.AbstractBlackBoxImpl;

import java.util.Collections;
import java.util.List;

/**
 * @param <T>
 * @author viswadas leher
 */
public class CassandraBlackBox<T extends BlackBoxable> extends AbstractBlackBoxImpl<T> implements BlackBox<T> {

    private CassandraConnectionManager connectionManager;

    public CassandraBlackBox(CassandraConnectionManager connectionManager) {
        this.connectionManager = connectionManager;
    }

    @Override
    public boolean delete(List<T> rows) throws BlackBoxException {
        createTable(rows);
        CassandraDeleteTask<T> deleteJob = new CassandraDeleteTask<>(getConnectionManager(), rows);
        return getForkJoinPool().invoke(deleteJob);
    }

    @Override
    public List<T> fetch(List<T> rows) throws BlackBoxException {
        CassandraFetchTask<T> fetchTask = new CassandraFetchTask<T>(getConnectionManager(), rows, false, -1);
        return getForkJoinPool().invoke(fetchTask);
    }

    @Override
    public List<T> fetchByPartialKey(List<T> rows, long maxResults) throws BlackBoxException {
        // TODO: really inefficient full table scan
        CassandraFetchTask<T> fetchTask = new CassandraFetchTask<T>(getConnectionManager(), rows, true, maxResults);
        return getForkJoinPool().invoke(fetchTask);
    }

    @Override
    public boolean insert(List<T> rows) throws BlackBoxException {
        createTable(rows);
        CassandraInsertTask<T> insertJob = new CassandraInsertTask<>(getConnectionManager(), rows, true);
        return getForkJoinPool().invoke(insertJob);
    }

    @Override
    public List<T> search(List<T> rows, long maxResults) throws BlackBoxException {
        createTable(rows);
        CassandraSearchTask<T> searchTask = new CassandraSearchTask<>(getConnectionManager(), rows, maxResults);
        return getForkJoinPool().invoke(searchTask);
    }

    @Override
    public List<T> search(T startRow, T endRow, long maxResults) throws BlackBoxException {
        createTable(Collections.singletonList(startRow));
        if (startRow.getClass().equals(endRow.getClass())) {
            CassandraRangeSearchTask<T> searchTask = new CassandraRangeSearchTask<>(getConnectionManager(), startRow, endRow, maxResults);
            return getForkJoinPool().invoke(searchTask);
        }
        return Collections.emptyList();
    }

    @Override
    public boolean update(List<T> rows) throws BlackBoxException {
        createTable(rows);
        CassandraInsertTask<T> insertJob = new CassandraInsertTask<>(getConnectionManager(), rows, false);
        return getForkJoinPool().invoke(insertJob);
    }

    /**
     * @param rows
     * @throws BlackBoxException
     */
    private void createTable(List<T> rows) {
        for (T row : rows) {
            new CassandraUtils<T>(connectionManager).createTable(row);
        }
    }

    /**
     * @return
     */
    private CassandraConnectionManager getConnectionManager() {
        return connectionManager;
    }
}
