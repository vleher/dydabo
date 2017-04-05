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
package com.dydabo.blackbox.cassandra;

import com.datastax.driver.core.Session;
import com.dydabo.blackbox.BlackBox;
import com.dydabo.blackbox.BlackBoxException;
import com.dydabo.blackbox.BlackBoxable;
import com.dydabo.blackbox.cassandra.tasks.CassandraDeleteTask;
import com.dydabo.blackbox.cassandra.tasks.CassandraFetchTask;
import com.dydabo.blackbox.cassandra.tasks.CassandraInsertTask;
import com.dydabo.blackbox.cassandra.tasks.CassandraRangeSearchTask;
import com.dydabo.blackbox.cassandra.tasks.CassandraSearchTask;
import com.dydabo.blackbox.cassandra.utils.CassandraUtils;
import com.dydabo.blackbox.db.CassandraConnectionManager;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ForkJoinPool;

/**
 *
 * @author viswadas leher <vleher@gmail.com>
 */
public class CassandraBlackBoxImpl<T extends BlackBoxable> implements BlackBox<T> {

    CassandraUtils utils = new CassandraUtils();

    @Override
    public boolean delete(List<T> rows) throws BlackBoxException {
        boolean successFlag = true;
        createTable(rows);
        ForkJoinPool fjPool = ForkJoinPool.commonPool();
        CassandraDeleteTask<T> deleteJob = new CassandraDeleteTask<>(getSession(), rows);
        Boolean flag = fjPool.invoke(deleteJob);
        successFlag = successFlag && flag;
        return successFlag;
    }

    @Override
    public boolean delete(T row) throws BlackBoxException {
        return delete(Arrays.asList(row));
    }

    @Override
    public List<T> fetch(List<String> rowKeys, T bean) throws BlackBoxException {
        createTable(Arrays.asList(bean));
        ForkJoinPool fjPool = ForkJoinPool.commonPool();
        CassandraFetchTask<T> searchTask = new CassandraFetchTask<>(getSession(), rowKeys, bean, false, -1);
        return fjPool.invoke(searchTask);
    }

    @Override
    public List<T> fetch(String rowKey, T bean) throws BlackBoxException {
        return fetch(Arrays.asList(rowKey), bean);
    }

    @Override
    public List<T> fetchByPartialKey(List<String> rowKeys, T bean) throws BlackBoxException {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public List<T> fetchByPartialKey(List<String> rowKeys, T bean, long maxResults) throws BlackBoxException {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public List<T> fetchByPartialKey(String rowKey, T bean) throws BlackBoxException {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public List<T> fetchByPartialKey(String rowKey, T bean, long maxResults) throws BlackBoxException {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public boolean insert(List<T> rows) throws BlackBoxException {
        boolean successFlag = true;
        createTable(rows);
        ForkJoinPool fjPool = ForkJoinPool.commonPool();
        CassandraInsertTask<T> insertJob = new CassandraInsertTask<>(getSession(), rows, true);
        boolean flag = fjPool.invoke(insertJob);
        successFlag = successFlag && flag;

        return successFlag;
    }

    @Override
    public boolean insert(T row) throws BlackBoxException {
        return insert(Arrays.asList(row));
    }

    @Override
    public List<T> search(List<T> rows) throws BlackBoxException {
        return search(rows, -1);
    }

    @Override
    public List<T> search(List<T> rows, long maxResults) throws BlackBoxException {
        createTable(rows);
        ForkJoinPool fjPool = ForkJoinPool.commonPool();
        CassandraSearchTask<T> searchTask = new CassandraSearchTask<>(getSession(), rows, maxResults);
        return fjPool.invoke(searchTask);
    }

    @Override
    public List<T> search(T row) throws BlackBoxException {
        return search(Arrays.asList(row));
    }

    @Override
    public List<T> search(T row, long maxResults) throws BlackBoxException {
        return search(Arrays.asList(row), maxResults);
    }

    @Override
    public List<T> search(T startRow, T endRow) throws BlackBoxException {
        return search(startRow, endRow, -1);
    }

    @Override
    public List<T> search(T startRow, T endRow, long maxResults) throws BlackBoxException {
        createTable(Arrays.asList(startRow));
        if (startRow.getClass().equals(endRow.getClass())) {
            ForkJoinPool fjPool = ForkJoinPool.commonPool();
            CassandraRangeSearchTask<T> searchTask = new CassandraRangeSearchTask<>(getSession(), startRow, endRow, maxResults);
            return fjPool.invoke(searchTask);
        }
        return Collections.<T>emptyList();
    }

    @Override
    public boolean update(List<T> rows) throws BlackBoxException {
        boolean successFlag = true;
        createTable(rows);
        ForkJoinPool fjPool = ForkJoinPool.commonPool();
        CassandraInsertTask<T> insertJob = new CassandraInsertTask<>(getSession(), rows, true);
        boolean flag = fjPool.invoke(insertJob);
        successFlag = successFlag && flag;

        return successFlag;
    }

    @Override
    public boolean update(T newRow) throws BlackBoxException {
        return update(Arrays.asList(newRow));
    }

    protected void createTable(List<T> rows) throws BlackBoxException {
        for (T row : rows) {
            new CassandraUtils<T>().createTable(row);
        }

    }

    protected Session getSession() {
        return CassandraConnectionManager.getSession("bb");
    }
}
