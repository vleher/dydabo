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
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.RecursiveTask;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

/**
 *
 * @author viswadas leher <vleher@gmail.com>
 */
public class HBaseInsertTask<T extends BlackBoxable> extends RecursiveTask<Boolean> {

    private final Connection connection;
    private final T row;
    private final HBaseUtils<T> utils;
    private final boolean checkExisting;

    public HBaseInsertTask(Connection connection, T row, boolean checkExisting) {
        this.connection = connection;
        this.row = row;
        this.checkExisting = checkExisting;
        this.utils = new HBaseUtils<T>();
    }

    protected Boolean insert(T row, boolean checkExisting) throws BlackBoxException {
        boolean successFlag = true;
        try (final Admin admin = getConnection().getAdmin()) {
            // consider create to be is nothing but alter...so
            utils.createTable(row, admin);
            try (final Table hTable = admin.getConnection().getTable(utils.getTableName(row))) {
                // We should "insert" only if there is no such or similar row.
                // If there is one, then the user should call update (not insert).
                if (checkExisting) {
                    boolean rowExists = utils.checkIfRowExists(row, hTable);
                    if (rowExists) {
                        throw new BlackBoxException("Cannot insert as the row exist. " + "Change the rowkey or call update. Current RowKey " + row.getBBRowKey());
                    }
                }
                // Find all the fields in the object
                Put put = new Put(Bytes.toBytes(row.getBBRowKey()));
                Map<String, String> valueMap = new HashMap<>();
                utils.convertJsonToMap(row, valueMap);
                for (String key : valueMap.keySet()) {
                    final String value = valueMap.get(key);
                    put.addColumn(Bytes.toBytes(HBaseJsonImpl.DEFAULT_FAMILY), Bytes.toBytes(key), Bytes.toBytes(value));
                }
                hTable.put(put);
            }
        } catch (IOException ex) {
            Logger.getLogger(HBaseJsonImpl.class.getName()).log(Level.SEVERE, null, ex);
            successFlag = false;
        }
        return successFlag;
    }

    @Override
    protected Boolean compute() {
        try {
            return insert(row, checkExisting);
        } catch (BlackBoxException ex) {
            Logger.getLogger(HBaseInsertTask.class.getName()).log(Level.SEVERE, null, ex);
        }
        return false;
    }

    public Connection getConnection() {
        return connection;
    }
}
