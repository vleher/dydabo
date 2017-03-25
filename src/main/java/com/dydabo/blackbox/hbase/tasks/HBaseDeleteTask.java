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
package com.dydabo.blackbox.hbase.tasks;

import com.dydabo.blackbox.BlackBoxException;
import com.dydabo.blackbox.BlackBoxable;
import com.dydabo.blackbox.hbase.HBaseJsonImpl;
import com.dydabo.blackbox.hbase.utils.HBaseUtils;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.RecursiveTask;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

/**
 *
 * @author viswadas leher <vleher@gmail.com>
 * @param <T>
 */
public class HBaseDeleteTask<T extends BlackBoxable> extends RecursiveTask<Boolean> {

    private final Connection connection;
    private final HBaseUtils utils;
    private List<T> rows;

    /**
     *
     * @param connection
     * @param row
     */
    public HBaseDeleteTask(Connection connection, T row) {
        this.connection = connection;
        this.rows = Arrays.asList(row);
        this.utils = new HBaseUtils<T>();
    }

    /**
     *
     * @param connection
     * @param rows
     */
    public HBaseDeleteTask(Connection connection, List<T> rows) {
        this.connection = connection;
        this.rows = rows;
        this.utils = new HBaseUtils<T>();
    }

    @Override
    protected Boolean compute() {
        try {
            return delete(rows);
        } catch (BlackBoxException ex) {
            Logger.getLogger(HBaseDeleteTask.class.getName()).log(Level.SEVERE, null, ex);
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
            HBaseDeleteTask<T> fDeleteJob = new HBaseDeleteTask<>(getConnection(), rows.subList(0, mid));
            HBaseDeleteTask<T> sDeleteJob = new HBaseDeleteTask<>(getConnection(), rows.subList(mid, rows.size()));
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
        try (Admin admin = getConnection().getAdmin()) {
            // consider create to be is nothing but alter...so
            try (Table hTable = admin.getConnection().getTable(utils.getTableName(row))) {
                if (utils.isValidRowKey(row)) {
                    Delete delete = new Delete(Bytes.toBytes(row.getBBRowKey()));
                    //delete.addFamily(Bytes.toBytes(HBaseTable.DEFAULT_FAMILY));
                    hTable.delete(delete);
                } else {
                    return false;
                }
            }
            return true;
        } catch (IOException ex) {
            Logger.getLogger(HBaseJsonImpl.class.getName()).log(Level.SEVERE, null, ex);
        }
        return false;
    }

    private Connection getConnection() {
        return this.connection;
    }
}
