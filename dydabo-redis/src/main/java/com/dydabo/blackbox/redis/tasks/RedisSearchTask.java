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
import com.dydabo.blackbox.common.utils.DyDaBoDBUtils;
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
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ForkJoinTask;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/**
 * @author viswadas leher
 */
public class RedisSearchTask<T extends BlackBoxable> extends RedisBaseTask<List<T>, T> {

    private final Logger logger = LogManager.getLogger();

    private final List<T> rows;
    private final int maxResults;
    private final RedisUtils<T> utils;
    private final boolean isFirst;

    public RedisSearchTask(RedisConnectionManager connectionManager, List<T> rows, int maxResults, boolean isFirst) {
        setConnectionManager(connectionManager);
        this.rows = rows;
        this.maxResults = maxResults;
        this.isFirst = isFirst;
        this.utils = new RedisUtils<>();
    }

    @Override
    protected List<T> compute() {
        return search(rows);
    }

    private List<T> search(List<T> rows) {
        if (rows.size() < DyDaBoDBUtils.MIN_PARALLEL_THRESHOLD) {
            final List<T> fullResult = new ArrayList<>();
            try (StatefulRedisConnection<String, String> connection = getConnectionManager().getConnection()) {
                for (T r : rows) {
                    List<T> res = search(r, connection);
                    fullResult.addAll(res);
                }
            }
            return fullResult;
        }

        List<ForkJoinTask<List<T>>> taskList = new ArrayList<>();
        ForkJoinTask<List<T>> fjTaskOne = new RedisSearchTask<>(getConnectionManager(), rows.subList(0, rows.size() / 2),
                maxResults, isFirst);
        taskList.add(fjTaskOne);
        ForkJoinTask<List<T>> fjTaskTwo = new RedisSearchTask<>(getConnectionManager(), rows.subList(rows.size() / 2,
                rows.size()), maxResults, isFirst);
        taskList.add(fjTaskTwo);

        return invokeAll(taskList).stream().map(ForkJoinTask::join).flatMap(Collection::stream).collect(Collectors.toList());
    }

    private List<T> search(T row, StatefulRedisConnection<String, String> connection) {
        List<T> results = new MaxResultList<>(maxResults);
        final String type = getRedisUtils().getRowKey(row, "");
        GenericDBTableRow tableRow = utils.convertRowToTableRow(row);
        RedisCommands<String, String> redisCommands = connection.sync();
        // A very inefficient search....Redis is not designed for this.
        String rowKey = getRedisUtils().getRowKey(row, "*");
        logger.debug("searching : {}", rowKey);
        List<String> allKeys = redisCommands.keys(rowKey);

        for (String key : allKeys) {
            if (key.startsWith(type)) {
                String currentRow = redisCommands.get(key);
                T rowObject = new Gson().fromJson(currentRow, (Type) row.getClass());
                if (compare(rowObject, tableRow)) {
                    results.add(rowObject);
                    if (isFirst && maxResults > 0 && results.size() >= maxResults) {
                        break;
                    }
                }
            }
        }

        return results;
    }

    private boolean compare(T rowObject, GenericDBTableRow tableRow) {
        GenericDBTableRow tableRowObject = utils.convertRowToTableRow(rowObject);

        for (Map.Entry<String, GenericDBTableRow.ColumnFamily> entry : tableRow.getColumnFamilies().entrySet()) {
            GenericDBTableRow.ColumnFamily colFam = entry.getValue();
            for (Map.Entry<String, GenericDBTableRow.Column> column : colFam.getColumns().entrySet()) {
                String colName = column.getKey();
                GenericDBTableRow.Column colValue = column.getValue();
                if (colValue != null && colValue.getColumnValue() != null) {
                    final String colString = colValue.getColumnValueAsString();
                    if (compareColumnStrings(tableRowObject, colFam, colName, colString)) {
                        return false;
                    }
                }
            }
        }

        return true;
    }

    private boolean compareColumnStrings(GenericDBTableRow tableRowObject, GenericDBTableRow.ColumnFamily colFam, String colName,
                                         String colString) {
        if (DyDaBoUtils.isValidRegex(colString)) {
            final String columnValueAsString =
                    tableRowObject.getColumnFamily(colFam.getFamilyName()).getColumn(colName).getColumnValueAsString();
            if (colString.startsWith("{") || colString.startsWith("[")) {
                // TODO: compare maps and arrays
            } else {
                Pattern p = Pattern.compile(colString);
                return DyDaBoUtils.isBlankOrNull(columnValueAsString) || !p.matcher(columnValueAsString).matches();
            }
        }
        return false;
    }
}
