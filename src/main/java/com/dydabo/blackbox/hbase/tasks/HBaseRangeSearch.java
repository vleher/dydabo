/*
 * Copyright 2017 viswadas leher <vleher@gmail.com>.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.dydabo.blackbox.hbase.tasks;

import com.dydabo.blackbox.BlackBoxException;
import com.dydabo.blackbox.BlackBoxable;
import com.dydabo.blackbox.common.DyDaBoUtils;
import com.dydabo.blackbox.hbase.obj.HBaseTable;
import com.dydabo.blackbox.hbase.utils.HBaseUtils;
import com.google.gson.Gson;
import com.google.gson.JsonSyntaxException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.RecursiveTask;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.BinaryComparator;
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
public class HBaseRangeSearch<T extends BlackBoxable> extends RecursiveTask<List<T>> {

    private final Connection connection;
    private T endRow;
    private final Logger logger = Logger.getLogger(HBaseRangeSearch.class.getName());
    private T startRow;
    private final HBaseUtils<T> utils;

    /**
     *
     * @param connection
     * @param startRow
     * @param endRow
     */
    public HBaseRangeSearch(Connection connection, T startRow, T endRow) {
        this.connection = connection;
        this.startRow = startRow;
        this.endRow = endRow;
        this.utils = new HBaseUtils<>();
    }

    @Override
    protected List<T> compute() {
        List<T> results = new ArrayList<>();
        try {
            FilterList filterList = new FilterList(FilterList.Operator.MUST_PASS_ALL);
            HBaseTable startTable = utils.convertRowToHTable(startRow, true);
            HBaseTable endTable = utils.convertRowToHTable(endRow, true);
            boolean hasFilters = parseForFilters(startTable, endTable, filterList);

            try (Admin admin = getConnection().getAdmin()) {
                try (Table hTable = admin.getConnection().getTable(utils.getTableName(startRow))) {
                    Scan scan = new Scan();

                    if (hasFilters) {
                        logger.info(filterList.toString());
                        scan.setFilter(filterList);
                    }

                    try (ResultScanner resultScanner = hTable.getScanner(scan)) {
                        for (Result result : resultScanner) {
                            HBaseTable resultTable = utils.parseResultToHTable(result, startRow);
                            T resultObject = new Gson().fromJson(resultTable.toJsonObject(), (Class<T>) startRow.getClass());
                            if (resultObject != null) {
                                results.add(resultObject);
                            }
                        }
                    }
                }
            } catch (IOException ex) {
                logger.log(Level.SEVERE, null, ex);
            }

        } catch (JsonSyntaxException ex) {
            logger.log(Level.SEVERE, null, ex);
        } catch (BlackBoxException ex) {
            logger.log(Level.SEVERE, null, ex);
        }
        return results;
    }

    /**
     *
     * @return
     */
    public Connection getConnection() {
        return connection;
    }

    /**
     *
     * @param startTable the value of startRow
     * @param endTable   the value of endRow
     * @param filterList the value of filterList
     */
    private boolean parseForFilters(HBaseTable startTable, HBaseTable endTable, FilterList filterList) {
        boolean hasFilters = false;

        for (Map.Entry<String, HBaseTable.ColumnFamily> entry : startTable.getColumnFamilies().entrySet()) {
            String familyName = entry.getKey();
            HBaseTable.ColumnFamily colfamily = entry.getValue();
            for (Map.Entry<String, HBaseTable.Column> col : colfamily.getColumns().entrySet()) {
                String colName = col.getKey();
                HBaseTable.Column colValue = col.getValue();
                final Object columnValue = colValue.getColumnValue();

                if (DyDaBoUtils.isNumber(columnValue)) {
                    // Binary Comparator Filter
//                    String regexValue = colValue.getColumnValueAsString();
//                    regexValue = utils.sanitizeRegex(regexValue);
                    if (colValue != null) {
                        BinaryComparator binaryComp = new BinaryComparator(utils.getAsByteArray(columnValue));
                        SingleColumnValueFilter startFilter = new SingleColumnValueFilter(Bytes.toBytes(familyName),
                                Bytes.toBytes(colName), CompareFilter.CompareOp.GREATER_OR_EQUAL, binaryComp);
                        HBaseTable.Column endValue = endTable.getColumnFamily(familyName).getColumn(colName);
                        if (endValue != null && endValue.getColumnValue() != null) {
//                            String endRegexValue = endValue.getColumnValueAsString();
//                            endRegexValue = utils.sanitizeRegex(endRegexValue);
                            //if (endRegexValue instanceof String && DyDaBoUtils.isValidRegex((String) endRegexValue)) {
                            BinaryComparator endBinaryComp = new BinaryComparator(utils.getAsByteArray(endValue.getColumnValue()));
                            SingleColumnValueFilter endFilter = new SingleColumnValueFilter(Bytes.toBytes(familyName),
                                    Bytes.toBytes(colName), CompareFilter.CompareOp.LESS, endBinaryComp);
                            filterList.addFilter(endFilter);
                            //}
                        }

                        filterList.addFilter(startFilter);
                        hasFilters = true;
                    }
                } else {
                    // Rgeular expression filter
                    String regexValue = colValue.getColumnValueAsString();
                    regexValue = utils.sanitizeRegex(regexValue);
                    if (regexValue instanceof String && DyDaBoUtils.isValidRegex((String) regexValue)) {

                        RegexStringComparator regexComp = new RegexStringComparator((String) regexValue);
                        SingleColumnValueFilter startFilter = new SingleColumnValueFilter(Bytes.toBytes(familyName),
                                Bytes.toBytes(colName), CompareFilter.CompareOp.GREATER_OR_EQUAL, regexComp);
                        HBaseTable.Column endValue = endTable.getColumnFamily(familyName).getColumn(colName);
                        if (endValue != null) {
                            String endRegexValue = endValue.getColumnValueAsString();
                            endRegexValue = utils.sanitizeRegex(endRegexValue);
                            if (endRegexValue instanceof String && DyDaBoUtils.isValidRegex((String) endRegexValue)) {
                                RegexStringComparator endRegexComp = new RegexStringComparator((String) endRegexValue);
                                SingleColumnValueFilter endFilter = new SingleColumnValueFilter(Bytes.toBytes(familyName),
                                        Bytes.toBytes(colName), CompareFilter.CompareOp.LESS, endRegexComp);
                                filterList.addFilter(endFilter);
                            }
                        }
                        filterList.addFilter(startFilter);
                        hasFilters = true;
                    }
                }

                System.out.println(colName + " :" + columnValue.getClass());

            }
        }

        return hasFilters;
    }

}