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
import com.dydabo.blackbox.common.utils.DyDaBoUtils;
import com.dydabo.blackbox.redis.db.RedisConnectionManager;
import io.lettuce.core.RedisFuture;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.async.RedisAsyncCommands;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ForkJoinTask;

/** @author viswadas leher */
public class RedisInsertTask<T extends BlackBoxable> extends RedisBaseTask<Boolean, T> {

  private final Logger logger = LogManager.getLogger();
  private final boolean checkExisting;

  private final List<T> rows;

  public RedisInsertTask(
      RedisConnectionManager connectionManager, List<T> rows, boolean checkExisting) {
    setConnectionManager(connectionManager);
    this.rows = rows;
    this.checkExisting = checkExisting;
  }

  @Override
  protected Boolean compute() {
    return insert(rows);
  }

  private Boolean insert(List<T> rows) {
    logger.debug("Inserting {} rows into database. checkExisting:{}", rows.size(), checkExisting);
    Boolean successFlag = Boolean.TRUE;
    if (rows.size() < DyDaBoDBUtils.MIN_PARALLEL_THRESHOLD) {
      try (StatefulRedisConnection<String, String> connection =
          getConnectionManager().getConnection()) {
        for (T t : rows) {
          successFlag = successFlag && insertRow(t, connection);
        }
      }
      return successFlag;
    }

    List<ForkJoinTask<Boolean>> taskList = new ArrayList<>();
    ForkJoinTask<Boolean> fjTaskOne =
        new RedisInsertTask<>(
            getConnectionManager(), rows.subList(0, rows.size() / 2), checkExisting);
    taskList.add(fjTaskOne);
    ForkJoinTask<Boolean> fjTaskTwo =
        new RedisInsertTask<>(
            getConnectionManager(), rows.subList(rows.size() / 2, rows.size()), checkExisting);
    taskList.add(fjTaskTwo);

    return invokeAll(taskList).stream()
        .map(ForkJoinTask::join)
        .reduce(Boolean::logicalAnd)
        .orElse(false);
  }

  private Boolean insertRow(T row, StatefulRedisConnection<String, String> connection) {
    final String key = getRedisUtils().getRowKey(row);
    logger.debug("Inserting a single row: {}", key);

    RedisAsyncCommands<String, String> redisCommands = connection.async();
    if (checkExisting) {
      RedisFuture<String> future = redisCommands.get(key);
      String currentRow = null;
      try {
        currentRow = future.get();
      } catch (InterruptedException | ExecutionException e) {
        logger.error("Cannot get an existing value for {}", key);
      }
      if (DyDaBoUtils.isNotBlankOrNull(currentRow)) {
        logger.debug("Not updating/inserting as it exist: {}", key);
        return false;
      }
    }
    String bbJson = row.getBBJson();
    logger.debug("Inserting {} => {}", key, bbJson);
    RedisFuture<String> future = redisCommands.set(key, bbJson);
    String result = null;
    try {
      result = future.get();
    } catch (InterruptedException | ExecutionException e) {
      logger.error("Cannot insert {} to the database", key);
    }
    return result != null;
  }
}
