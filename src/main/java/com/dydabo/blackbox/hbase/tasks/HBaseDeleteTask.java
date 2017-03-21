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
import java.util.concurrent.RecursiveTask;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

import static com.dydabo.blackbox.hbase.HBaseJsonImpl.DEFAULT_FAMILY;

/**
 *
 * @author viswadas leher <vleher@gmail.com>
 */
public class HBaseDeleteTask<T extends BlackBoxable> extends RecursiveTask<Boolean> {

    private final T row;
    private final Connection connection;
    private final HBaseUtils utils;

    public HBaseDeleteTask(Connection connection, T row) {
        this.connection = connection;
        this.row = row;
        this.utils = new HBaseUtils<T>();
    }

    @Override
    protected Boolean compute() {
        try {
            return delete(row);
        } catch (BlackBoxException ex) {
            Logger.getLogger(HBaseDeleteTask.class.getName()).log(Level.SEVERE, null, ex);
        }
        return false;
    }

    protected Boolean delete(T row) throws BlackBoxException {
        try (Admin admin = getConnection().getAdmin()) {
            // consider create to be is nothing but alter...so
            utils.createTable(row, admin);
            try (Table hTable = admin.getConnection().getTable(utils.getTableName(row))) {
                Delete delete = new Delete(Bytes.toBytes(row.getBBRowKey()));
                delete.addFamily(Bytes.toBytes(DEFAULT_FAMILY));
                hTable.delete(delete);
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
