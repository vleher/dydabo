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

package com.dydabo.blackbox.redis.tasks;

import com.dydabo.blackbox.BlackBoxable;
import com.dydabo.blackbox.common.MaxResultList;
import com.dydabo.blackbox.common.utils.DyDaBoUtils;
import com.dydabo.blackbox.db.obj.GenericDBTableRow;
import com.dydabo.blackbox.redis.db.RedisConnectionManager;
import com.dydabo.blackbox.redis.utils.RedisUtils;
import com.google.gson.Gson;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.sync.RedisCommands;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.lang.reflect.Type;
import java.math.BigDecimal;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * @author viswadas leher
 */
public class RedisRangeSearchTask<T extends BlackBoxable> extends RedisBaseTask<List<T>, T> {

    private final Logger logger = LogManager.getLogger();
    private final T startRow;
    private final T endRow;
    private final int maxResults;
    private final RedisUtils<T> utils;
    private final boolean isFirst;

    public RedisRangeSearchTask(RedisConnectionManager connectionManager, T startRow, T endRow, int maxResults, boolean isFirst) {
        setConnectionManager(connectionManager);
        this.startRow = startRow;
        this.endRow = endRow;
        this.maxResults = maxResults;
        this.isFirst = isFirst;
        this.utils = new RedisUtils<>();
    }

    @Override
    protected List<T> compute() {
        return search(startRow, endRow, maxResults, isFirst);
    }

    private List<T> search(T startRow, T endRow, int maxResults, boolean isFirst) {
        List<T> results = new MaxResultList<>(maxResults);
        GenericDBTableRow startTableRow = utils.convertRowToTableRow(startRow);
        GenericDBTableRow endTableRow = utils.convertRowToTableRow(endRow);

        // An inefficient search that scans all rows
        try (StatefulRedisConnection<String, String> connection = getConnectionManager().getConnection()) {
            RedisCommands<String, String> redisCommands = connection.sync();
            List<String> allKeys = redisCommands.keys(getRedisUtils().getRowKey(startRow, "*"));

            for (String key : allKeys) {
                String currentRow = redisCommands.get(key);
                T rowObject = new Gson().fromJson(currentRow, (Type) startRow.getClass());

                if (filter(rowObject, startTableRow, endTableRow)) {
                    results.add(rowObject);
                    if (isFirst && maxResults > 0 && results.size() >= maxResults) {
                        break;
                    }
                }
            }
        }

        return results;
    }

    private boolean filter(T rowObject, GenericDBTableRow startTableRow, GenericDBTableRow endTableRow) {
        GenericDBTableRow tableRowObject = utils.convertRowToTableRow(rowObject);

        for (Map.Entry<String, GenericDBTableRow.ColumnFamily> familyEntry : tableRowObject.getColumnFamilies().entrySet()) {
            GenericDBTableRow.ColumnFamily colFamily = familyEntry.getValue();
            for (Map.Entry<String, GenericDBTableRow.Column> columnEntry : colFamily.getColumns().entrySet()) {
                String columnName = columnEntry.getKey();
                GenericDBTableRow.Column columnValue = columnEntry.getValue();

                GenericDBTableRow.Column startColValue =
                        startTableRow.getColumnFamily(colFamily.getFamilyName()).getColumn(columnName);
                GenericDBTableRow.Column endColValue = endTableRow.getColumnFamily(colFamily.getFamilyName()).getColumn(columnName);

                if (!compareInRange(startColValue, columnValue, endColValue)) {
                    return false;
                }
            }
        }

        return true;
    }

    // TODO : refactor this
    private boolean compareInRange(GenericDBTableRow.Column startColValue, GenericDBTableRow.Column columnValue,
                                   GenericDBTableRow.Column endColValue) {
        Object s1 = null;
        Object s2 = null;
        Object s3 = null;

        if (startColValue != null && DyDaBoUtils.isNotBlankOrNull(startColValue.getColumnValueAsString())) {
            s1 = startColValue.getColumnValue();
        }

        if (columnValue != null && DyDaBoUtils.isNotBlankOrNull(columnValue.getColumnValueAsString())) {
            s2 = columnValue.getColumnValue();
        }

        if (endColValue != null && DyDaBoUtils.isNotBlankOrNull(endColValue.getColumnValueAsString())) {
            s3 = endColValue.getColumnValue();
        }
        boolean flag = false;

        if (s1 == null && s3 == null) {
            return true;
        }

        if (Objects.equals(DyDaBoUtils.EMPTY_ARRAY, Objects.requireNonNull(startColValue).getColumnValueAsString()) && Objects.equals(DyDaBoUtils.EMPTY_ARRAY, Objects.requireNonNull(endColValue).getColumnValueAsString())) {
            flag = true;
        }

        if (Objects.equals(DyDaBoUtils.EMPTY_MAP, startColValue.getColumnValueAsString()) && Objects.equals(DyDaBoUtils.EMPTY_MAP
                , Objects.requireNonNull(endColValue).getColumnValueAsString())) {
            flag = true;
        }

        if (flag) {
            return true;
        }

        flag = compareNumbersAndStrings(s1, s2) && compareNumbersAndStrings(s2, s3);

        return flag;
    }

    private boolean compareNumbersAndStrings(Object first, Object second) {
        boolean flag = false;
        if (first != null && second != null) {
            if (first instanceof Number && second instanceof Number) {
                if (compareTo((Number) first, (Number) second) <= 0) {
                    flag = true;
                }
            } else if (first instanceof String && second instanceof String) {
                if (DyDaBoUtils.isARegex((String) first)) {
                    flag = ((String) second).matches((String) first);
                } else if (((String) first).compareTo((String) second) <= 0) {
                    flag = true;
                }
            }
        }
        return flag;
    }

    private int compareTo(Number n1, Number n2) {
        // ignoring null handling
        return BigDecimal.valueOf(n1.doubleValue()).compareTo(BigDecimal.valueOf(n2.doubleValue()));
    }
}
