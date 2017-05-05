/*
 * Copyright 2017 viswadas leher <vleher@gmail.com>.
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
package com.dydabo.blackbox.hbase.tasks;

import com.dydabo.blackbox.BlackBoxException;
import com.dydabo.blackbox.BlackBoxable;
import com.dydabo.blackbox.hbase.utils.HBaseUtils;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ForkJoinTask;
import java.util.concurrent.RecursiveTask;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * @param <T>
 * @author viswadas leher <vleher@gmail.com>
 */
public class HBaseDeleteTask<T extends BlackBoxable> extends RecursiveTask<Boolean> {

    private final Connection connection;
    private final Logger logger = Logger.getLogger(HBaseDeleteTask.class.getName());
    private final HBaseUtils<T> utils;
    private final List<T> rows;

    /**
     * @param connection
     * @param row
     */
    public HBaseDeleteTask(Connection connection, T row) {
        this(connection, Collections.singletonList(row));
    }

    /**
     * @param connection
     * @param rows
     */
    public HBaseDeleteTask(Connection connection, List<T> rows) {
        this.connection = connection;
        this.rows = rows;
        this.utils = new HBaseUtils<>();
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
     * @param rows
     * @return
     * @throws BlackBoxException
     */
    protected Boolean delete(List<T> rows) throws BlackBoxException {
        if (rows.size() < 2) {
            Boolean successFlag = true;
            for (T t : rows) {
                successFlag = successFlag && delete(t);
            }
            return successFlag;
        }

        Boolean successFlag = Boolean.TRUE;
        // create a task for each element or row in the list
        List<ForkJoinTask<Boolean>> taskList = new ArrayList<>();
        for (T row : rows) {
            ForkJoinTask<Boolean> fjTask = new HBaseDeleteTask<>(getConnection(), Collections.singletonList(row)).fork();
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
     * @return
     * @throws BlackBoxException
     */
    protected Boolean delete(T row) throws BlackBoxException {
        try (Admin admin = getConnection().getAdmin()) {
            // consider create to be is nothing but alter...so
            try (Table hTable = admin.getConnection().getTable(utils.getTableName(row))) {
                if (utils.isValidRowKey(row)) {
                    Delete delete = new Delete(Bytes.toBytes(row.getBBRowKey()));
                    // delete.addFamily(Bytes.toBytes(HBaseTable.DEFAULT_FAMILY));
                    hTable.delete(delete);
                } else {
                    return false;
                }
            }
            return true;
        } catch (IOException ex) {
            logger.log(Level.SEVERE, null, ex);
        }
        return false;
    }

    private Connection getConnection() {
        return this.connection;
    }
}
