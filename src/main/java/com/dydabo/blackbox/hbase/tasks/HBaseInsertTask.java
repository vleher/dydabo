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
import com.dydabo.blackbox.hbase.obj.HBaseTable;
import com.dydabo.blackbox.hbase.utils.HBaseUtils;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
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
    private final HBaseUtils<T> utils;
    private final boolean checkExisting;
    private List<T> rows;

    public HBaseInsertTask(Connection connection, T row, boolean checkExisting) {
        this.connection = connection;
        this.rows = Arrays.asList(row);
        this.checkExisting = checkExisting;
        this.utils = new HBaseUtils<T>();
    }

    public HBaseInsertTask(Connection connection, List<T> rows, boolean checkExisting) {
        this.connection = connection;
        this.rows = rows;
        this.checkExisting = checkExisting;
        this.utils = new HBaseUtils<T>();
    }

    protected Boolean insert(List<T> rows, boolean checkExisting) throws BlackBoxException {
        if (rows.size() < 2) {
            Boolean successFlag = Boolean.TRUE;
            for (T t : rows) {
                successFlag = successFlag && insert(t, checkExisting);
            }
            return successFlag;
        } else {
            Boolean successFlag = Boolean.TRUE;
            int mid = rows.size() / 2;
            HBaseInsertTask<T> fInsTask = new HBaseInsertTask<>(getConnection(), rows.subList(0, mid), checkExisting);
            HBaseInsertTask<T> sInsTask = new HBaseInsertTask<>(getConnection(), rows.subList(mid, rows.size()), checkExisting);
            fInsTask.fork();
            Boolean secondFlag = sInsTask.compute();
            Boolean firstFlag = fInsTask.join();
            successFlag = successFlag && secondFlag && firstFlag;
            return successFlag;
        }
    }

    protected Boolean insert(T row, boolean checkExisting) throws BlackBoxException {
        boolean successFlag = true;
        try (final Admin admin = getConnection().getAdmin()) {
            // consider create to be is nothing but alter...so
            try (final Table hTable = admin.getConnection().getTable(utils.getTableName(row))) {
                // We should "insert" only if there is no such or similar row.
                // If there is one, then the user should call update (not insert).
                if (checkExisting) {
                    boolean rowExists = utils.checkIfRowExists(row, hTable);
                    if (rowExists) {
                        throw new BlackBoxException("Cannot insert as the row exist. " +
                                "Change the rowkey or call update. Current RowKey " + row.getBBRowKey());
                    }
                }
                // Find all the fields in the object

                HBaseTable thisTable = utils.convertRowToHTable(row, true);
                Put put = new Put(Bytes.toBytes(row.getBBRowKey()));
                for (Map.Entry<String, HBaseTable.ColumnFamily> entry : thisTable.getColumnFamilies().entrySet()) {

                    String familyName = entry.getKey();
                    HBaseTable.ColumnFamily colFamily = entry.getValue();
                    for (Map.Entry<String, HBaseTable.Column> entry1 : colFamily.getColumns().entrySet()) {
                        String colName = entry1.getKey();
                        HBaseTable.Column colValue = entry1.getValue();
                        put.addColumn(Bytes.toBytes(familyName), Bytes.toBytes(colName), Bytes.toBytes(colValue.getColumnValue()));
                    }

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
            return insert(rows, checkExisting);
        } catch (BlackBoxException ex) {
            Logger.getLogger(HBaseInsertTask.class.getName()).log(Level.SEVERE, null, ex);
        }
        return false;
    }

    public Connection getConnection() {
        return connection;
    }
}
