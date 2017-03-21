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
import com.dydabo.blackbox.hbase.utils.HBaseUtils;
import com.google.gson.Gson;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
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

import static com.dydabo.blackbox.hbase.HBaseJsonImpl.DEFAULT_FAMILY;

/**
 *
 * @author viswadas leher <vleher@gmail.com>
 */
public class HBaseFetchTask<T extends BlackBoxable> extends RecursiveTask<List<T>> {

    private final Connection connection;
    private final T row;
    private final HBaseUtils<T> utils;

    public HBaseFetchTask(Connection connection, T row) {
        this.connection = connection;
        this.row = row;
        this.utils = new HBaseUtils<T>();
    }

    protected List<T> fetch(T row) throws BlackBoxException {
        List<T> results = new ArrayList<>();

        try (Admin admin = getConnection().getAdmin()) {
            // consider create to be is nothing but alter...so
            utils.createTable(row, admin);
            try (Table hTable = admin.getConnection().getTable(utils.getTableName(row))) {
                Scan scan = new Scan();
                // Get the filters
                Map<String, String> valueMap = new HashMap<>();
                valueMap = utils.convertJsonToMap(row, valueMap);

                FilterList filterList = new FilterList(FilterList.Operator.MUST_PASS_ALL);
                for (Map.Entry<String, String> entry : valueMap.entrySet()) {
                    String columnName = entry.getKey();
                    String regexValue = entry.getValue();
                    if (!DyDaBoUtils.isBlankOrNull(columnName, regexValue)) {
                        RegexStringComparator regexComp = new RegexStringComparator(regexValue);
                        SingleColumnValueFilter scvf = new SingleColumnValueFilter(Bytes.toBytes(DEFAULT_FAMILY),
                                Bytes.toBytes(columnName), CompareFilter.CompareOp.EQUAL, regexComp);
                        filterList.addFilter(scvf);
                    }
                }
                scan.setFilter(filterList);

                try (ResultScanner resultScanner = hTable.getScanner(scan)) {
                    for (Result result = resultScanner.next(); result != null; result = resultScanner.next()) {
                        HashMap<String, String> valueTable = new HashMap<>();

                        for (Cell listCell : result.listCells()) {
                            String value = Bytes.toString(CellUtil.cloneValue(listCell));
                            String keyName = Bytes.toString(CellUtil.cloneQualifier(listCell));
                            valueTable.put(keyName, value);
                        }

                        String jsonString = utils.generateJson(valueTable);

                        T resultObject = new Gson().fromJson(jsonString, (Class<T>) row.getClass());
                        results.add(resultObject);
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
            return fetch(row);
        } catch (BlackBoxException ex) {
            Logger.getLogger(HBaseFetchTask.class.getName()).log(Level.SEVERE, null, ex);
        }
        return new ArrayList<>();
    }

    public Connection getConnection() {
        return connection;
    }

}
