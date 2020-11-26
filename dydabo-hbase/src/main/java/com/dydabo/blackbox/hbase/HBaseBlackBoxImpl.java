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
package com.dydabo.blackbox.hbase;

import com.dydabo.blackbox.BlackBox;
import com.dydabo.blackbox.BlackBoxException;
import com.dydabo.blackbox.BlackBoxable;
import com.dydabo.blackbox.common.AbstractBlackBoxImpl;
import com.dydabo.blackbox.hbase.db.HBaseConnectionManager;
import com.dydabo.blackbox.hbase.tasks.impl.*;
import com.dydabo.blackbox.hbase.utils.HBaseUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Connection;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * @param <T>
 * @author viswadas leher
 */
public class HBaseBlackBoxImpl<T extends BlackBoxable> extends AbstractBlackBoxImpl<T> implements BlackBox<T> {

    private final Configuration config;
    private final Logger logger = Logger.getLogger(HBaseBlackBoxImpl.class.getName());
    private HBaseUtils<T> hBaseUtils = new HBaseUtils<>();

    /**
     * @throws IOException
     */
    public HBaseBlackBoxImpl() {
        this.config = HBaseConfiguration.create();
    }

    /**
     * @param config
     * @throws java.io.IOException
     */
    public HBaseBlackBoxImpl(Configuration config) {
        this.config = config;
    }

    /**
     * @param rows
     * @throws BlackBoxException
     */
    private void createTable(List<T> rows) throws BlackBoxException {
        for (T row : rows) {
            try {
                hBaseUtils.createTable(row, getConnection());
            } catch (IOException ex) {
                logger.log(Level.SEVERE, null, ex);
                throw new BlackBoxException(ex.getMessage());
            }
        }
    }

    @Override
    public boolean delete(List<T> rows) throws BlackBoxException {
        createTable(rows);
        try {
            HBaseDeleteTask<T> deleteJob = new HBaseDeleteTask<>(getConnection(), rows);
            List<T> result = getForkJoinPool().invoke(deleteJob);
            return !result.isEmpty();
        } catch (IOException ex) {
            logger.log(Level.SEVERE, null, ex);
        }
        return false;
    }

    @Override
    public List<T> fetch(List<T> rows) throws BlackBoxException {
        try {
            HBaseFetchTask<T> fetchTask = new HBaseFetchTask<T>(getConnection(), rows, false);
            return getForkJoinPool().invoke(fetchTask);
        } catch (IOException ex) {
            logger.log(Level.SEVERE, null, ex);
        }
        return new ArrayList<>();
    }

    @Override
    public List<T> fetchByPartialKey(List<T> rows,  long maxResults) throws BlackBoxException {
        try {
            HBaseFetchTask<T> fetchTask = new HBaseFetchTask<T>(getConnection(), rows, true, maxResults);
            return getForkJoinPool().invoke(fetchTask);
        } catch (IOException ex) {
            logger.log(Level.SEVERE, null, ex);
        }
        return Collections.emptyList();
    }

    /**
     * @return
     * @throws java.io.IOException
     */
    private Connection getConnection() throws IOException {
        return new HBaseConnectionManager().getConnection(config);
    }

    @Override
    public boolean insert(List<T> rows) throws BlackBoxException {
        createTable(rows);
        try {
            HBaseInsertTask<T> insertJob = new HBaseInsertTask<>(getConnection(), rows, true);
            List<T> result = getForkJoinPool().invoke(insertJob);
            return !result.isEmpty();
        } catch (IOException ex) {
            logger.log(Level.SEVERE, null, ex);
        }

        return false;
    }

    @Override
    public List<T> search(List<T> rows, long maxResults) throws BlackBoxException {
        createTable(rows);
        try {
            HBaseSearchTask<T> searchTask = new HBaseSearchTask<>(getConnection(), rows, maxResults);
            return getForkJoinPool().invoke(searchTask);
        } catch (IOException ex) {
            logger.log(Level.SEVERE, null, ex);
        }
        return Collections.emptyList();
    }

    @Override
    public List<T> search(T startRow, T endRow, long maxResults) throws BlackBoxException {
        createTable(Collections.singletonList(startRow));
        if (startRow.getClass().equals(endRow.getClass())) {
            try {
                HBaseRangeSearchTask<T> searchTask = new HBaseRangeSearchTask<>(getConnection(), startRow, endRow, maxResults);
                return getForkJoinPool().invoke(searchTask);
            } catch (IOException ex) {
                logger.log(Level.SEVERE, null, ex);
            }
        }
        return Collections.emptyList();
    }

    @Override
    public boolean update(List<T> rows) throws BlackBoxException {
        createTable(rows);
        try {
            HBaseInsertTask<T> insertJob = new HBaseInsertTask<>(getConnection(), rows, false);
            List<T> result = getForkJoinPool().invoke(insertJob);
            return !result.isEmpty();
        } catch (IOException ex) {
            logger.log(Level.SEVERE, null, ex);
        }

        return false;
    }

    public HBaseUtils<T> gethBaseUtils() {
        return hBaseUtils;
    }

    public void sethBaseUtils(HBaseUtils<T> hBaseUtils) {
        this.hBaseUtils = hBaseUtils;
    }
}
