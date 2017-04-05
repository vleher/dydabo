/** *****************************************************************************
 * Copyright 2017 viswadas leher <vleher@gmail.com>.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 ******************************************************************************
 */
package com.dydabo.blackbox.hbase;

import com.dydabo.blackbox.BlackBox;
import com.dydabo.blackbox.BlackBoxException;
import com.dydabo.blackbox.BlackBoxable;
import com.dydabo.blackbox.db.HBaseConnectionManager;
import com.dydabo.blackbox.hbase.tasks.HBaseDeleteTask;
import com.dydabo.blackbox.hbase.tasks.HBaseFetchTask;
import com.dydabo.blackbox.hbase.tasks.HBaseInsertTask;
import com.dydabo.blackbox.hbase.tasks.HBaseRangeSearchTask;
import com.dydabo.blackbox.hbase.tasks.HBaseSearchTask;
import com.dydabo.blackbox.hbase.utils.HBaseUtils;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ForkJoinPool;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Connection;

/**
 *
 * @author viswadas leher <vleher@gmail.com>
 * @param <T>
 */
public class HBaseBlackBoxImpl<T extends BlackBoxable> implements BlackBox<T> {

    private final Configuration config;
    private final Logger logger = Logger.getLogger(HBaseBlackBoxImpl.class.getName());

    /**
     *
     * @throws IOException
     */
    public HBaseBlackBoxImpl() throws IOException {
        this.config = HBaseConfiguration.create();
    }

    /**
     *
     * @param config
     *
     * @throws java.io.IOException
     */
    public HBaseBlackBoxImpl(Configuration config) throws IOException {
        this.config = config;
    }

    /**
     *
     * @param rows
     *
     * @throws BlackBoxException
     */
    protected void createTable(List<T> rows) throws BlackBoxException {
        for (T row : rows) {
            try {
                new HBaseUtils<T>().createTable(row, getConnection());
            } catch (IOException ex) {
                logger.log(Level.SEVERE, null, ex);
                throw new BlackBoxException(ex.getMessage());
            }
        }

    }

    @Override
    public boolean delete(List<T> rows) throws BlackBoxException {
        boolean successFlag = true;
        createTable(rows);
        try {
            ForkJoinPool fjPool = ForkJoinPool.commonPool();
            HBaseDeleteTask<T> deleteJob = new HBaseDeleteTask<>(getConnection(), rows);
            Boolean flag = fjPool.invoke(deleteJob);
            successFlag = successFlag && flag;
        } catch (IOException ex) {
            logger.log(Level.SEVERE, null, ex);
        }
        return successFlag;
    }

    @Override
    public boolean delete(T row) throws BlackBoxException {
        return delete(Arrays.asList(row));
    }

    @Override
    public List<T> fetch(List<String> rowKeys, T row) throws BlackBoxException {
        ForkJoinPool fjPool = ForkJoinPool.commonPool();
        try {
            HBaseFetchTask<T> fetchTask = new HBaseFetchTask<>(getConnection(), rowKeys, row, false);
            return fjPool.invoke(fetchTask);
        } catch (IOException ex) {
            logger.log(Level.SEVERE, null, ex);
        }
        return new ArrayList<>();
    }

    @Override
    public List<T> fetch(String rowKey, T bean) throws BlackBoxException {
        return fetch(Arrays.asList(rowKey), bean);
    }

    @Override
    public List<T> fetchByPartialKey(List<String> rowKeys, T bean) throws BlackBoxException {
        return fetchByPartialKey(rowKeys, bean, -1);
    }

    @Override
    public List<T> fetchByPartialKey(String rowKey, T bean) throws BlackBoxException {
        return fetchByPartialKey(Arrays.asList(rowKey), bean, -1);
    }

    @Override
    public List<T> fetchByPartialKey(List<String> rowKeys, T bean, long maxResults) throws BlackBoxException {
        ForkJoinPool fjPool = ForkJoinPool.commonPool();
        try {
            HBaseFetchTask<T> fetchTask = new HBaseFetchTask<>(getConnection(), rowKeys, bean, true, maxResults);
            return fjPool.invoke(fetchTask);
        } catch (IOException ex) {
            logger.log(Level.SEVERE, null, ex);
        }
        return Collections.emptyList();
    }

    @Override
    public List<T> fetchByPartialKey(String rowKey, T bean, long maxResults) throws BlackBoxException {
        return fetchByPartialKey(Arrays.asList(rowKey), bean, maxResults);
    }

    /**
     *
     * @return
     * @throws java.io.IOException
     */
    public Connection getConnection() throws IOException {
        return HBaseConnectionManager.getConnection(config);
    }

    @Override
    public boolean insert(List<T> rows) throws BlackBoxException {
        boolean successFlag = true;
        createTable(rows);
        ForkJoinPool fjPool = ForkJoinPool.commonPool();
        try {
            HBaseInsertTask<T> insertJob = new HBaseInsertTask<>(getConnection(), rows, true);
            boolean flag = fjPool.invoke(insertJob);
            successFlag = successFlag && flag;
        } catch (IOException ex) {
            logger.log(Level.SEVERE, null, ex);
        }

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
    public List<T> search(T startRow, T endRow) throws BlackBoxException {
        return search(startRow, endRow, -1);
    }

    @Override
    public List<T> search(T row) throws BlackBoxException {
        return search(Arrays.asList(row));
    }

    @Override
    public List<T> search(List<T> rows, long maxResults) throws BlackBoxException {
        createTable(rows);
        ForkJoinPool fjPool = ForkJoinPool.commonPool();
        try {
            HBaseSearchTask<T> searchTask = new HBaseSearchTask<>(getConnection(), rows, maxResults);
            return fjPool.invoke(searchTask);
        } catch (IOException ex) {
            logger.log(Level.SEVERE, null, ex);
        }
        return Collections.<T>emptyList();
    }

    @Override
    public List<T> search(T row, long maxResults) throws BlackBoxException {
        return search(Arrays.asList(row), maxResults);
    }

    @Override
    public List<T> search(T startRow, T endRow, long maxResults) throws BlackBoxException {
        createTable(Arrays.asList(startRow));
        if (startRow.getClass().equals(endRow.getClass())) {
            ForkJoinPool fjPool = ForkJoinPool.commonPool();
            try {
                HBaseRangeSearchTask<T> searchTask = new HBaseRangeSearchTask<>(getConnection(), startRow, endRow, maxResults);
                return fjPool.invoke(searchTask);
            } catch (IOException ex) {
                logger.log(Level.SEVERE, null, ex);
            }
        }
        return Collections.<T>emptyList();
    }

    @Override
    public boolean update(List<T> rows) throws BlackBoxException {
        boolean successFlag = true;
        createTable(rows);
        ForkJoinPool fjPool = ForkJoinPool.commonPool();
        try {
            HBaseInsertTask<T> insertJob = new HBaseInsertTask<>(getConnection(), rows, false);
            boolean flag = fjPool.invoke(insertJob);
            successFlag = successFlag && flag;
        } catch (IOException ex) {
            logger.log(Level.SEVERE, null, ex);
        }

        return successFlag;
    }

    @Override
    public boolean update(T newRow) throws BlackBoxException {
        return update(Arrays.asList(newRow));
    }

}
