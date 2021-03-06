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
import com.dydabo.blackbox.common.utils.DyDaBoUtils;
import com.dydabo.blackbox.redis.db.RedisConnectionManager;
import com.google.gson.Gson;
import redis.clients.jedis.Jedis;

import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ForkJoinTask;
import java.util.concurrent.RecursiveTask;
import java.util.logging.Logger;

/**
 * @author viswadas leher
 */
public class RedisFetchTask<T extends BlackBoxable> extends RecursiveTask<List<T>> {

    private final List<T> rows;
    private final boolean isPartialKey;
    private final long maxResults;
    private Logger logger = Logger.getLogger(RedisFetchTask.class.getName());


    public RedisFetchTask(List<T> rows, boolean isPartialKey, long maxResults) {
        this.rows = rows;
        this.isPartialKey = isPartialKey;
        this.maxResults = maxResults;
    }

    @Override
    protected List<T> compute() {
        return fetch(rows);
    }

    private List<T> fetch(List<T> rows) {
        if (rows.size() < 2) {
            List<T> fullResult = new ArrayList<>();
            for (T row : rows) {
                fullResult.addAll(fetch(row));
            }
            return fullResult;
        }

        List<T> fullResult = new ArrayList<>();

        List<ForkJoinTask<List<T>>> taskList = new ArrayList<>();
        for (T row : rows) {
            ForkJoinTask<List<T>> fjTask = new RedisFetchTask<>(Collections.singletonList(row), isPartialKey, maxResults).fork();
            taskList.add(fjTask);
        }

        for (ForkJoinTask<List<T>> forkJoinTask : taskList) {
            fullResult.addAll(forkJoinTask.join());
        }

        return fullResult;
    }

    private List<T> fetch(T row) {
        List<T> fullResults = Collections.synchronizedList(new ArrayList<>());

        if (DyDaBoUtils.isBlankOrNull(row.getBBRowKey())) {
            return fullResults;
        }

        String type = row.getClass().getTypeName() + ":";

        try (Jedis connection = RedisConnectionManager.getConnection("localhost")) {
            if (isPartialKey) {
                String partialKey = row.getBBRowKey().replaceAll("\\.\\*", "*");
                Set<String> newKeys = connection.keys(type + partialKey);

                for (String newKey : newKeys) {
                    String result = connection.get(newKey);
                    if (!DyDaBoUtils.isBlankOrNull(result)) {
                        T resultObj = new Gson().fromJson(result, (Type) row.getClass());
                        fullResults.add(resultObj);
                        if (maxResults > 0 && fullResults.size() >= maxResults) {
                            return fullResults;
                        }
                    }
                }
            } else {
                String result = connection.get(type + row.getBBRowKey());
                if (!DyDaBoUtils.isBlankOrNull(result)) {
                    T resultObj = new Gson().fromJson(result, (Type) row.getClass());
                    fullResults.add(resultObj);
                }
            }
        }

        return fullResults;
    }
}
