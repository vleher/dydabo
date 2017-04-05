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
package com.dydabo.blackbox.hbase.tasks;

import com.dydabo.blackbox.BlackBoxException;
import com.dydabo.blackbox.BlackBoxable;
import com.dydabo.blackbox.hbase.obj.HBaseTableRow;
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
import org.apache.hadoop.hbase.regionserver.NoSuchColumnFamilyException;
import org.apache.hadoop.hbase.util.Bytes;

/**
 *
 * @author viswadas leher <vleher@gmail.com>
 * @param <T>
 */
public class HBaseInsertTask<T extends BlackBoxable> extends RecursiveTask<Boolean> {

    private static final Logger logger = Logger.getLogger(HBaseInsertTask.class.getName());

    private final Connection connection;
    private final HBaseUtils<T> utils;
    private final boolean checkExisting;
    private List<T> rows;

    /**
     *
     * @param connection
     * @param row
     * @param checkExisting
     */
    public HBaseInsertTask(Connection connection, T row, boolean checkExisting) {
        this(connection, Arrays.asList(row), checkExisting);
    }

    /**
     *
     * @param connection
     * @param rows
     * @param checkExisting
     */
    public HBaseInsertTask(Connection connection, List<T> rows, boolean checkExisting) {
        this.connection = connection;
        this.rows = rows;
        this.checkExisting = checkExisting;
        this.utils = new HBaseUtils<T>();
    }

    /**
     *
     * @param rows
     * @param checkExisting
     *
     * @return
     *
     * @throws BlackBoxException
     */
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

    /**
     *
     * @param row
     * @param checkExisting
     *
     * @return
     *
     * @throws BlackBoxException
     */
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
                HBaseTableRow thisTable = utils.convertRowToHTable(row, true);
                if (utils.isValidRowKey(row)) {
                    Put put = new Put(Bytes.toBytes(row.getBBRowKey()));
                    for (Map.Entry<String, HBaseTableRow.ColumnFamily> entry : thisTable.getColumnFamilies().entrySet()) {
                        String familyName = entry.getKey();
                        HBaseTableRow.ColumnFamily colFamily = entry.getValue();
                        for (Map.Entry<String, HBaseTableRow.Column> column : colFamily.getColumns().entrySet()) {
                            String colName = column.getKey();
                            HBaseTableRow.Column colValue = column.getValue();
                            Object thisValue = colValue.getColumnValue();
                            byte[] byteArray = utils.getAsByteArray(thisValue);

                            if (byteArray != null) {
                                put.addColumn(Bytes.toBytes(familyName), Bytes.toBytes(colName), byteArray);
                            }

                        }
                    }

                    try {
                        hTable.put(put);
                    } catch (NoSuchColumnFamilyException ncfEx) {
                        // TODO: try altering the table....
                        utils.alterTable(row, connection);
                        successFlag = false;
                    }
                } else {
                    successFlag = false;
                }
            }
        } catch (IOException ex) {
            logger.log(Level.SEVERE, null, ex);
            successFlag = false;
        }
        return successFlag;
    }

    @Override
    protected Boolean compute() {
        try {
            return insert(rows, checkExisting);
        } catch (BlackBoxException ex) {
            logger.log(Level.SEVERE, null, ex);
        }
        return false;
    }

    /**
     *
     * @return
     */
    public Connection getConnection() {
        return connection;
    }
}
