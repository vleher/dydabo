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
package com.dydabo.blackbox.hbase.tasks.impl;

import com.dydabo.blackbox.BlackBoxException;
import com.dydabo.blackbox.BlackBoxable;
import com.dydabo.blackbox.common.utils.DyDaBoDBUtils;
import com.dydabo.blackbox.hbase.tasks.HBaseTask;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ForkJoinTask;

/**
 * @param <T>
 * @author viswadas leher
 */
public class HBaseDeleteTask<T extends BlackBoxable> extends HBaseTask<T, Boolean> {

    private final Logger logger = LogManager.getLogger();
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
        super(connection);
        this.rows = rows;
    }

    @Override
    protected Boolean compute() {
        try {
            return delete(rows);
        } catch (BlackBoxException ex) {
            logger.error(ex);
        }
        return false;
    }

    /**
     * @param rows
     * @return
     * @throws BlackBoxException
     */
    private Boolean delete(List<T> rows) throws BlackBoxException {
        if (rows.size() < DyDaBoDBUtils.MIN_PARALLEL_THRESHOLD) {
            boolean successFlag = true;
            for (T t : rows) {
                successFlag = successFlag && delete(t);
            }
            return successFlag;
        }

        boolean successFlag = Boolean.TRUE;
        // create a task for each element or row in the list
        List<ForkJoinTask<Boolean>> taskList = new ArrayList<>();
        for (T row : rows) {
            ForkJoinTask<Boolean> fjTask = new HBaseDeleteTask<>(getConnection(), Collections.singletonList(row));
            taskList.add(fjTask);
        }
        return ForkJoinTask.invokeAll(taskList).stream().map(ForkJoinTask::join).reduce(Boolean::logicalAnd).orElse(false);
    }

    /**
     * @param row
     * @return
     * @throws BlackBoxException
     */
    private boolean delete(T row) {
        try (Table hTable = getConnection().getTable(getUtils().getTableName(row))) {
            if (getUtils().isValidRowKey(row)) {
                Delete delete = new Delete(Bytes.toBytes(row.getBBRowKey()));
                // delete.addFamily(Bytes.toBytes(HBaseTable.DEFAULT_FAMILY));
                hTable.delete(delete);
            } else {
                return false;
            }
        } catch (IOException e) {
            logger.error(e);
            return false;
        }
        return true;
    }
}
