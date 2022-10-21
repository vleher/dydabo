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
package com.dydabo.blackbox.cassandra;

import com.dydabo.blackbox.BlackBox;
import com.dydabo.blackbox.BlackBoxException;
import com.dydabo.blackbox.BlackBoxable;
import com.dydabo.blackbox.cassandra.db.CassandraConnectionManager;
import com.dydabo.blackbox.cassandra.tasks.CassandraDeleteTask;
import com.dydabo.blackbox.cassandra.tasks.CassandraFetchTask;
import com.dydabo.blackbox.cassandra.tasks.CassandraInsertTask;
import com.dydabo.blackbox.cassandra.tasks.CassandraRangeSearchTask;
import com.dydabo.blackbox.cassandra.tasks.CassandraSearchTask;
import com.dydabo.blackbox.cassandra.utils.CassandraUtils;
import com.dydabo.blackbox.common.AbstractBlackBoxImpl;

import java.util.Collections;
import java.util.List;

/**
 * @param <T>
 * @author viswadas leher
 */
public class CassandraBlackBox<T extends BlackBoxable> extends AbstractBlackBoxImpl<T> implements BlackBox<T> {

    private final CassandraConnectionManager connectionManager;

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
        createTable(rows);
        CassandraFetchTask<T> fetchTask = new CassandraFetchTask<>(getConnectionManager(), rows, false, Integer.MAX_VALUE, false);
        return getForkJoinPool().invoke(fetchTask);
    }

    @Override
    public List<T> fetchByPartialKey(List<T> rows, int maxResults, boolean isFirst) throws BlackBoxException {
        // TODO: really inefficient full table scan
        CassandraFetchTask<T> fetchTask = new CassandraFetchTask<>(getConnectionManager(), rows, true, maxResults, isFirst);
        return getForkJoinPool().invoke(fetchTask);
    }

    @Override
    public boolean insert(List<T> rows) throws BlackBoxException {
        createTable(rows);
        CassandraInsertTask<T> insertJob = new CassandraInsertTask<>(getConnectionManager(), rows, true);
        return getForkJoinPool().invoke(insertJob);
    }

    @Override
    public List<T> search(List<T> rows, int maxResults, boolean isFirst) throws BlackBoxException {
        createTable(rows);
        CassandraSearchTask<T> searchTask = new CassandraSearchTask<>(getConnectionManager(), rows, maxResults, isFirst);
        return getForkJoinPool().invoke(searchTask);
    }

    @Override
    public List<T> search(T startRow, T endRow, int maxResults, boolean isFirst) throws BlackBoxException {
        createTable(Collections.singletonList(startRow));
        if (startRow.getClass().equals(endRow.getClass())) {
            CassandraRangeSearchTask<T> searchTask = new CassandraRangeSearchTask<>(getConnectionManager(), startRow, endRow,
                    maxResults, isFirst);
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
     * @param rows list of objects
     */
    private void createTable(List<T> rows) {
        rows.forEach(row -> new CassandraUtils<T>(connectionManager).createTable(row));
    }

    /**
     * @return if connection cannot be created
     */
    private CassandraConnectionManager getConnectionManager() {
        return connectionManager;
    }
}
