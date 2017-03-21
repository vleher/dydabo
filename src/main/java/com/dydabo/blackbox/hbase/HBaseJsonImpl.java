/*
 * Copyright (C) 2017 viswadas leher <vleher@gmail.com>
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package com.dydabo.blackbox.hbase;

import com.dydabo.blackbox.BlackBox;
import com.dydabo.blackbox.BlackBoxException;
import com.dydabo.blackbox.BlackBoxable;
import com.dydabo.blackbox.db.HBaseConnectionManager;
import com.dydabo.blackbox.hbase.tasks.HBaseDeleteTask;
import com.dydabo.blackbox.hbase.tasks.HBaseInsertTask;
import com.dydabo.blackbox.hbase.tasks.HBaseFetchTask;
import java.io.IOException;
import java.util.ArrayList;
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
 */
public class HBaseJsonImpl<T extends BlackBoxable> implements BlackBox<T> {

    // The ColumnFamily names should be as small as possible for performance
    public static final String DEFAULT_FAMILY = "D";
    private final Configuration config;

    /**
     *
     */
    public HBaseJsonImpl() throws IOException {
        this.config = HBaseConfiguration.create();
    }

    /**
     *
     * @param config
     */
    public HBaseJsonImpl(Configuration config) throws IOException {
        this.config = config;
    }

    /**
     *
     * @return
     */
    public Connection getConnection() throws IOException {
        return HBaseConnectionManager.getConnection(config);
    }

    @Override
    public boolean delete(List<T> rows) throws BlackBoxException {
        boolean successFlag = true;

        ForkJoinPool fjPool = new ForkJoinPool();
        for (T t : rows) {
            try {
                HBaseDeleteTask<T> deleteJob = new HBaseDeleteTask<>(getConnection(), t);
                Boolean flag = fjPool.invoke(deleteJob);
                successFlag = successFlag && flag;
            } catch (IOException ex) {
                Logger.getLogger(HBaseJsonImpl.class.getName()).log(Level.SEVERE, null, ex);
            }
        }

        return successFlag;
    }

    @Override
    public boolean insert(List<T> rows) throws BlackBoxException {
        boolean successFlag = true;

        ForkJoinPool fjPool = new ForkJoinPool();
        for (T t : rows) {
            try {
                HBaseInsertTask<T> insertJob = new HBaseInsertTask<>(getConnection(), t, true);
                boolean flag = fjPool.invoke(insertJob);
                successFlag = successFlag && flag;
            } catch (IOException ex) {
                Logger.getLogger(HBaseJsonImpl.class.getName()).log(Level.SEVERE, null, ex);
            }
        }

        return successFlag;
    }

    @Override
    public List<T> fetch(List<T> rows) throws BlackBoxException {
        List<T> combinedResults = new ArrayList<>();

        ForkJoinPool fjPool = new ForkJoinPool();
        for (T t : rows) {
            try {
                HBaseFetchTask<T> fetchTask = new HBaseFetchTask<>(getConnection(), t);
                List<T> results = fjPool.invoke(fetchTask);
                combinedResults.addAll(results);
            } catch (IOException ex) {
                Logger.getLogger(HBaseJsonImpl.class.getName()).log(Level.SEVERE, null, ex);
            }
        }
        return combinedResults;
    }

    @Override
    public boolean update(List<T> rows) throws BlackBoxException {
        boolean successFlag = true;

        ForkJoinPool fjPool = new ForkJoinPool();

        for (T t : rows) {
            try {
                HBaseInsertTask<T> insertJob = new HBaseInsertTask<>(getConnection(), t, false);
                boolean flag = fjPool.invoke(insertJob);
                successFlag = successFlag && flag;
            } catch (IOException ex) {
                Logger.getLogger(HBaseJsonImpl.class.getName()).log(Level.SEVERE, null, ex);
            }
        }

        return successFlag;
    }

    private void info(String msg) {
        Logger.getLogger(BlackBox.class.getName()).log(Level.INFO, msg);
    }

}
