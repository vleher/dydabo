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
import com.dydabo.blackbox.db.RedisConnectionManager;
import redis.clients.jedis.Jedis;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ForkJoinTask;
import java.util.concurrent.RecursiveTask;
import java.util.logging.Logger;

/**
 * @author viswadas leher
 */
public class RedisInsertTask<T extends BlackBoxable> extends RecursiveTask<Boolean> {

    private static final Logger logger = Logger.getLogger(RedisInsertTask.class.getName());
    private final boolean checkExisting;

    private List<T> rows = null;

    public RedisInsertTask(List<T> rows, boolean checkExisting) {
        this.rows = rows;
        this.checkExisting = checkExisting;
    }

    @Override
    protected Boolean compute() {
        return insert(rows);
    }

    private Boolean insert(List<T> rows) {
        Boolean successFlag = Boolean.TRUE;
        if (rows.size() < 2) {
            for (T t : rows) {
                successFlag = successFlag && insert(t, checkExisting);
            }
            return successFlag;
        }

        // create a task for each element or row in the list
        List<ForkJoinTask<Boolean>> taskList = new ArrayList<>();
        for (T row : rows) {
            ForkJoinTask<Boolean> fjTask = new RedisInsertTask<>(Collections.singletonList(row), checkExisting).fork();
            taskList.add(fjTask);
        }
        // wait for all to join
        for (ForkJoinTask<Boolean> forkJoinTask : taskList) {
            successFlag = successFlag && forkJoinTask.join();
        }

        return successFlag;
    }

    private Boolean insert(T row, boolean checkExisting) {
        try (Jedis connection = RedisConnectionManager.getConnection("localhost")) {
            String result = connection.set(row.getClass().getTypeName() + ":" + row.getBBRowKey(), row.getBBJson());
        }
        return true;
    }
}
