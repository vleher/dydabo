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
import com.dydabo.blackbox.common.utils.DyDaBoDBUtils;
import com.dydabo.blackbox.redis.db.RedisConnectionManager;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.sync.RedisCommands;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ForkJoinTask;

/**
 * @author viswadas leher
 */
public class RedisDeleteTask<T extends BlackBoxable> extends RedisBaseTask<Boolean, T> {

    private final Logger logger = LogManager.getLogger();

    private final List<T> rowsToDelete;

    public RedisDeleteTask(RedisConnectionManager connectionManager, List<T> rowsToDelete) {
        setConnectionManager(connectionManager);
        this.rowsToDelete = rowsToDelete;
    }

    @Override
    protected Boolean compute() {
        return delete(rowsToDelete);
    }

    private Boolean delete(List<T> rows) {
        Boolean successFlag = Boolean.TRUE;
        if (rows.size() < DyDaBoDBUtils.MIN_PARALLEL_THRESHOLD) {
            try (StatefulRedisConnection<String, String> connection = getConnectionManager().getConnection()) {
                RedisCommands<String, String> redisCommands = connection.sync();
                for (T t : rows) {
                    String rowKey = getRedisUtils().getRowKey(t);
                    if (rowKey.contains("*")) {
                        List<String> keyList = redisCommands.keys(rowKey);
                        keyList.forEach(redisCommands::del);
                    } else {
                        Long r = redisCommands.del(rowKey);
                        logger.debug("Deleting {} : {}", t.getBBRowKey(), r);
                    }
                }
            }
            return successFlag;
        }

        List<ForkJoinTask<Boolean>> taskList = new ArrayList<>();
        ForkJoinTask<Boolean> fjTaskOne = new RedisDeleteTask<>(getConnectionManager(), rows.subList(0, rows.size() / 2));
        taskList.add(fjTaskOne);
        ForkJoinTask<Boolean> fjTaskTwo = new RedisDeleteTask<>(getConnectionManager(), rows.subList(rows.size() / 2, rows.size()));
        taskList.add(fjTaskTwo);

        return invokeAll(taskList).stream().map(ForkJoinTask::join).reduce(Boolean::logicalAnd).orElse(false);
    }
}
