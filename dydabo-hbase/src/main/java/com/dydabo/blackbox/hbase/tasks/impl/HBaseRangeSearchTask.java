/*
 * Copyright 2017 viswadas leher .
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
package com.dydabo.blackbox.hbase.tasks.impl;

import com.dydabo.blackbox.BlackBoxable;
import com.dydabo.blackbox.common.utils.DyDaBoUtils;
import com.dydabo.blackbox.db.obj.GenericDBTableRow;
import com.dydabo.blackbox.hbase.tasks.HBaseTask;
import com.google.gson.Gson;
import com.google.gson.JsonSyntaxException;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * @param <T>
 * @author viswadas leher
 */
public class HBaseRangeSearchTask<T extends BlackBoxable> extends HBaseTask<T> {

    private final T endRow;
    private final Logger logger = Logger.getLogger(HBaseRangeSearchTask.class.getName());
    private final long maxResults;
    private final T startRow;

    /**
     * @param connection
     * @param startRow
     * @param endRow
     * @param maxResults
     */
    public HBaseRangeSearchTask(Connection connection, T startRow, T endRow, long maxResults) {
        super(connection);
        this.startRow = startRow;
        this.endRow = endRow;
        this.maxResults = maxResults;
    }

    @Override
    protected List<T> compute() {
        List<T> results = new ArrayList<>();
        try {
            FilterList filterList = new FilterList(FilterList.Operator.MUST_PASS_ALL);
            GenericDBTableRow startTable = utils.convertRowToTableRow(startRow);
            GenericDBTableRow endTable = utils.convertRowToTableRow(endRow);
            boolean hasFilters = parseForFilters(startTable, endTable, filterList);

            try (Admin admin = getConnection().getAdmin()) {
                try (Table hTable = admin.getConnection().getTable(utils.getTableName(startRow))) {
                    Scan scan = new Scan();

                    if (hasFilters) {
                        logger.finest(filterList.toString());
                        scan.setFilter(filterList);
                    }

                    try (ResultScanner resultScanner = hTable.getScanner(scan)) {
                        for (Result result : resultScanner) {
                            GenericDBTableRow resultTable = utils.parseResultToHTable(result, startRow);
                            T resultObject = new Gson().fromJson(resultTable.toJsonObject(), (Type) startRow.getClass());
                            if (resultObject != null) {
                                if (maxResults < 0) {
                                    results.add(resultObject);
                                } else if (maxResults > 0 && results.size() < maxResults) {
                                    results.add(resultObject);
                                } else {
                                    break;
                                }
                            }
                        }
                    }
                }
            } catch (IOException ex) {
                logger.log(Level.SEVERE, null, ex);
            }
        } catch (JsonSyntaxException ex) {
            logger.log(Level.SEVERE, null, ex);
        }
        return results;
    }

    /**
     * @param startTable the value of startRow
     * @param endTable   the value of endRow
     * @param filterList the value of filterList
     */
    private boolean parseForFilters(GenericDBTableRow startTable, GenericDBTableRow endTable, FilterList filterList) {
        final AtomicBoolean hasFilters = new AtomicBoolean(false);

        startTable.forEach((familyName, columnName, columnValue, columnValueAsString) -> {
            if (columnValue != null && DyDaBoUtils.isNumber(columnValue)) {
                // Binary Comparator Filter
                parseBinaryFilters(endTable, filterList, familyName, columnName, columnValue);
                hasFilters.set(true);
            } else {
                // Regular expression filter
                String regexValue = utils.sanitizeRegex(columnValueAsString);
                if (regexValue != null && DyDaBoUtils.isValidRegex(regexValue)) {
                    filterList.addFilter(parseRegexFilters(endTable, filterList, familyName, columnName, regexValue));
                    hasFilters.set(true);
                }
            }
        });

        return hasFilters.get();
    }

    private SingleColumnValueFilter parseRegexFilters(GenericDBTableRow endTable, FilterList filterList, String familyName, String columnName, String regexValue) {
        RegexStringComparator regexComp = new RegexStringComparator(regexValue);
        SingleColumnValueFilter startFilter = new SingleColumnValueFilter(Bytes.toBytes(familyName),
                Bytes.toBytes(columnName), CompareFilter.CompareOp.GREATER_OR_EQUAL, regexComp);
        GenericDBTableRow.Column endValue = endTable.getColumnFamily(familyName).getColumn(columnName);
        if (endValue != null) {
            String endRegexValue = endValue.getColumnValueAsString();
            endRegexValue = utils.sanitizeRegex(endRegexValue);
            if (endRegexValue != null && DyDaBoUtils.isValidRegex(endRegexValue)) {
                RegexStringComparator endRegexComp = new RegexStringComparator(endRegexValue);
                SingleColumnValueFilter endFilter = new SingleColumnValueFilter(Bytes.toBytes(familyName),
                        Bytes.toBytes(columnName), CompareFilter.CompareOp.LESS, endRegexComp);
                filterList.addFilter(endFilter);
            }
        }
        return startFilter;
    }

    private void parseBinaryFilters(GenericDBTableRow endTable, FilterList filterList, String familyName, String columnName, Object columnValue) {
        BinaryComparator binaryComp = new BinaryComparator(utils.getAsByteArray(columnValue));
        SingleColumnValueFilter startFilter = new SingleColumnValueFilter(Bytes.toBytes(familyName),
                Bytes.toBytes(columnName), CompareFilter.CompareOp.GREATER_OR_EQUAL, binaryComp);
        GenericDBTableRow.Column endValue = endTable.getColumnFamily(familyName).getColumn(columnName);
        if (endValue != null && endValue.getColumnValue() != null) {
            BinaryComparator endBinaryComp = new BinaryComparator(utils.getAsByteArray(endValue.getColumnValue()));
            SingleColumnValueFilter endFilter = new SingleColumnValueFilter(Bytes.toBytes(familyName),
                    Bytes.toBytes(columnName), CompareFilter.CompareOp.LESS, endBinaryComp);
            filterList.addFilter(endFilter);
        }

        filterList.addFilter(startFilter);
    }
}
