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

package com.dydabo.blackbox.redis.tasks;

import com.dydabo.blackbox.BlackBoxable;
import com.dydabo.blackbox.common.DyDaBoUtils;
import com.dydabo.blackbox.db.RedisConnectionManager;
import com.dydabo.blackbox.db.obj.GenericDBTableRow;
import com.dydabo.blackbox.redis.utils.RedisUtils;
import com.google.gson.Gson;
import org.mortbay.util.SingletonList;
import redis.clients.jedis.Jedis;

import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ForkJoinTask;
import java.util.concurrent.RecursiveTask;
import java.util.logging.Logger;
import java.util.regex.Pattern;

/**
 * @author viswadas leher
 */
public class RedisSearchTask<T extends BlackBoxable> extends RecursiveTask<List<T>> {

    private static Logger logger = Logger.getLogger(RedisSearchTask.class.getName());

    private final List<T> rows;
    private final long maxResults;
    private RedisUtils utils = null;


    public RedisSearchTask(List<T> rows, long maxResults) {
        this.rows = rows;
        this.maxResults = maxResults;
        this.utils = new RedisUtils();
    }

    @Override
    protected List<T> compute() {
        return search(rows);
    }

    private List<T> search(List<T> rows) {
        if (rows.size() < 2) {
            List<T> fullResult = new ArrayList<>();
            for (T r : rows) {
                List<T> res = search(r);
                fullResult.addAll(res);
            }
            return fullResult;
        }

        List<T> fullResult = new ArrayList<>();

        List<ForkJoinTask<List<T>>> taskList = new ArrayList<>();

        for (T r : rows) {
            ForkJoinTask<List<T>> sTask = new RedisSearchTask<>(SingletonList.newSingletonList(r), maxResults).fork();
            taskList.add(sTask);
        }

        for (ForkJoinTask<List<T>> t : taskList) {
            fullResult.addAll(t.join());
        }

        return fullResult;
    }

    private List<T> search(T row) {
        List<T> results = new ArrayList<>();
        final String type = row.getClass().getTypeName() + ":";
        GenericDBTableRow tableRow = utils.convertRowToTableRow(row);

        // A very inefficient search....Redis is not designed for this.
        try (Jedis connection = RedisConnectionManager.getConnection("localhost")) {
            Set<String> allKeys = connection.keys(type + "*");

            for (String key : allKeys) {
                if (key.startsWith(type.toString())) {
                    String currentRow = connection.get(key);

                    T rowObject = new Gson().fromJson(currentRow, (Type) row.getClass());
                    if (compare(rowObject, tableRow)) {
                        results.add(rowObject);
                        if (maxResults > 0 && results.size() >= maxResults) {
                            break;
                        }
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
                    if (DyDaBoUtils.isValidRegex(colString)) {
                        final String columnValueAsString = tableRowObject.getColumnFamily(colFam.getFamilyName()).getColumn(colName).getColumnValueAsString();
                        if (colString.startsWith("{") || colString.startsWith("[")) {
                            // TODO: compare maps and arrays
                        } else {
                            Pattern p = Pattern.compile(colString);
                            if (DyDaBoUtils.isBlankOrNull(columnValueAsString) || !p.matcher(columnValueAsString).matches()) {
                                return false;
                            }
                        }
                    }
                }
            }
        }

        return true;
    }
}
