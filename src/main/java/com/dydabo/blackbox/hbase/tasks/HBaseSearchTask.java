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
import com.dydabo.blackbox.common.DyDaBoUtils;
import com.dydabo.blackbox.hbase.HBaseJsonImpl;
import com.dydabo.blackbox.hbase.obj.HBaseTable;
import com.dydabo.blackbox.hbase.utils.HBaseUtils;
import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonPrimitive;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.RecursiveTask;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.RegexStringComparator;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.util.Bytes;

/**
 *
 * @author viswadas leher <vleher@gmail.com>
 * @param <T>
 */
public class HBaseSearchTask<T extends BlackBoxable> extends RecursiveTask<List<T>> {

    private final Connection connection;
    private final HBaseUtils<T> utils;
    private List<T> rows;

    /**
     *
     * @param connection
     * @param row
     */
    public HBaseSearchTask(Connection connection, T row) {
        this.connection = connection;
        this.rows = Arrays.asList(row);
        this.utils = new HBaseUtils<T>();
    }

    /**
     *
     * @param connection
     * @param rows
     */
    public HBaseSearchTask(Connection connection, List<T> rows) {
        this.connection = connection;
        this.rows = rows;
        this.utils = new HBaseUtils<T>();
    }

    /**
     *
     * @param rows
     *
     * @return
     *
     * @throws BlackBoxException
     */
    protected List<T> search(List<T> rows) throws BlackBoxException {
        if (rows.size() < 2) {
            List<T> fullResult = new ArrayList<>();
            for (T row : rows) {
                fullResult.addAll(search(row));
            }
            return fullResult;
        } else {
            List<T> fullResult = new ArrayList<>();
            int mid = rows.size() / 2;
            HBaseSearchTask<T> fDeleteJob = new HBaseSearchTask<>(getConnection(), rows.subList(0, mid));
            HBaseSearchTask<T> sDeleteJob = new HBaseSearchTask<>(getConnection(), rows.subList(mid, rows.size()));
            fDeleteJob.fork();
            List<T> secondResults = sDeleteJob.compute();
            List<T> firstResults = fDeleteJob.join();
            fullResult.addAll(firstResults);
            fullResult.addAll(secondResults);
            return fullResult;
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
    protected List<T> search(T row) throws BlackBoxException {
        List<T> results = new ArrayList<>();

        try (Admin admin = getConnection().getAdmin()) {
            // consider create to be is nothing but alter...so
            try (Table hTable = admin.getConnection().getTable(utils.getTableName(row))) {
                Scan scan = new Scan();
                // Get the filters : just simple filters for now
                HBaseTable thisTable = utils.convertRowToHTable(row, false);

                FilterList filterList = new FilterList(FilterList.Operator.MUST_PASS_ALL);
                boolean hasFilters = false;
                for (Map.Entry<String, HBaseTable.ColumnFamily> colFamEntry : thisTable.getColumnFamilies().entrySet()) {
                    String familyName = colFamEntry.getKey();
                    HBaseTable.ColumnFamily colFamily = colFamEntry.getValue();
                    for (Map.Entry<String, HBaseTable.Column> columnEntry : colFamily.getColumns().entrySet()) {
                        String colName = columnEntry.getKey();
                        HBaseTable.Column colValue = columnEntry.getValue();
                        String regexValue = colValue.getColumnValue();
                        if (regexValue instanceof String && DyDaBoUtils.isValidRegex((String) regexValue)) {
                            RegexStringComparator regexComp = new RegexStringComparator((String) regexValue);
                            SingleColumnValueFilter scvf = new SingleColumnValueFilter(Bytes.toBytes(familyName),
                                    Bytes.toBytes(colName), CompareFilter.CompareOp.EQUAL, regexComp);
                            filterList.addFilter(scvf);
                            hasFilters = true;
                        }

                    }
                }
                if (hasFilters) {
                    scan.setFilter(filterList);
                }

                try (ResultScanner resultScanner = hTable.getScanner(scan)) {
                    for (Result result = resultScanner.next(); result != null; result = resultScanner.next()) {
                        JsonObject jsonObject = new JsonObject();

                        for (Cell listCell : result.listCells()) {
                            String value = Bytes.toString(CellUtil.cloneValue(listCell));
                            String keyName = Bytes.toString(CellUtil.cloneQualifier(listCell));
                            JsonElement elem = DyDaBoUtils.parseJsonString(value);
                            if (elem != null) {
                                jsonObject.add(keyName, elem);
                            } else {
                                jsonObject.add(keyName, new JsonPrimitive(value));
                            }
                        }

                        T resultObject = new Gson().fromJson(jsonObject, (Class<T>) row.getClass());
                        if (resultObject != null) {
                            results.add(resultObject);
                        }
                    }
                }
            }
        } catch (IOException ex) {
            Logger.getLogger(HBaseJsonImpl.class.getName()).log(Level.SEVERE, null, ex);
        }
        return results;
    }

    @Override
    protected List<T> compute() {
        try {
            return search(rows);
        } catch (BlackBoxException ex) {
            Logger.getLogger(HBaseSearchTask.class.getName()).log(Level.SEVERE, null, ex);
        }
        return new ArrayList<>();
    }

    /**
     *
     * @return
     */
    public Connection getConnection() {
        return connection;
    }

}